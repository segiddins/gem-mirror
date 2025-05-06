use std::{
    cell::RefCell,
    collections::HashMap,
    ffi::OsStr,
    fs::{self},
    io::Read as _,
    path::{Path, PathBuf},
    sync::RwLock,
};

use miette::{bail, miette};
use serde::{Deserialize, Serialize};
use ssri::Integrity;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gem {
    pub full_name: String,
    pub name: String,
    pub version: String,
    pub platform: String,
    pub package_integrity: Integrity,
    metadata_gz_integrity: Option<Integrity>,
    pub stored: bool,
}

impl PartialEq for Gem {
    fn eq(&self, other: &Self) -> bool {
        self.full_name == other.full_name
    }
}

impl Eq for Gem {}

impl PartialOrd for Gem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Gem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.full_name.cmp(&other.full_name)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Namespace {
    name: String,
    info_checksum: String,
    pub versions: HashMap<String, Gem>,
}

impl Namespace {
    fn merge(&mut self, other: &Namespace) {
        let mut versions = other.versions.clone();
        for (key, version) in self.versions.iter() {
            if let Some(other_version) = versions.get(key) {
                if version.stored && !other_version.stored {
                    versions.insert(key.clone(), version.clone());
                }
            } else {
                versions.insert(key.clone(), version.clone());
            }
        }
        self.versions = versions;
    }
}

impl PartialEq for Namespace {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Namespace {}
impl PartialOrd for Namespace {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Namespace {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Index {
    pub source: String,
    pub gems: HashMap<String, Namespace>,
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}
impl Eq for Index {}

impl PartialOrd for Index {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Index {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.source.cmp(&other.source)
    }
}

pub trait Store {
    fn list_indices(&self) -> miette::Result<Vec<Index>>;
    fn add_index(&mut self, source: String) -> miette::Result<()> {
        self.with_indices(|_, indices| {
            if indices.iter().any(|i| i.source == source) {
                return Ok(());
            }
            indices.push(Index {
                source,
                gems: Default::default(),
            });
            Ok(())
        })?;
        Ok(())
    }
    fn store_blob<B: AsRef<[u8]>>(&self, blob: B) -> miette::Result<Integrity>;
    fn get_blob(&self, sha256: &Integrity) -> miette::Result<Vec<u8>>;
    fn has_blob(&self, sha256: &Integrity) -> bool {
        let blobs = self.get_blob(sha256);
        blobs.is_ok()
    }
    fn with_indices<F>(&mut self, f: F) -> miette::Result<Vec<Index>>
    where
        F: FnOnce(&mut Self, &mut Vec<Index>) -> miette::Result<()>;
}

pub fn update_store<T: Store>(mut store: T) -> miette::Result<()> {
    store.with_indices(|store, indices| {
        for index in indices {
            println!("Index source: {}", index.source);
            let mut versions_url = index.source.clone();
            versions_url.push_str("/versions");
            let resp = reqwest::blocking::get(&versions_url).unwrap();
            if resp.status() != reqwest::StatusCode::OK {
                bail!("Failed to fetch {}: {}", versions_url, resp.status());
            }
            let text = resp.text().unwrap();
            let mut versions = text.lines().collect::<Vec<_>>();
            if let Some((idx, _)) = versions
                .iter()
                .enumerate()
                .find(|(_, name)| **name == "---")
            {
                versions = versions[idx + 1..].to_vec();
            } else {
                bail!("Failed to find separator in versions");
            }
            let versions = {
                let mut h = HashMap::<&str, &str>::new();
                for line in versions {
                    let parts = line.split(" ").collect::<Vec<_>>();
                    let name = parts[0];
                    let info_checksum = parts[parts.len() - 1];
                    h.insert(name, info_checksum);
                }
                h
            };
            for (name, info_checksum) in versions {
                let existing = index.gems.get(name);
                if let Some(existing) = existing {
                    if existing.info_checksum == info_checksum
                        || (existing.info_checksum.starts_with('"')
                            && existing.info_checksum.ends_with('"')
                            && existing.info_checksum[1..existing.info_checksum.len() - 1]
                                == *info_checksum)
                    {
                        continue;
                    }
                }
                if existing.is_some_and(|n| n.info_checksum == info_checksum) {
                    // println!("Already stored {} {}", name, info_checksum);
                    continue;
                } else {
                    // eprintln!("New gem: {} {} vs {:?}", name, info_checksum, existing);
                }

                let gem_url = format!("{}/info/{}", index.source, name);
                let resp = match reqwest::blocking::get(&gem_url) {
                    Ok(resp) => resp,
                    Err(e) => {
                        eprintln!("Failed to fetch {}: {}", gem_url, e);
                        continue;
                    }
                };
                let mut info_checksum = resp.headers().get("ETag").unwrap().to_str().unwrap();
                info_checksum = info_checksum.trim_start_matches("W/");
                info_checksum = info_checksum.trim_matches('"');
                let info_checksum = info_checksum.to_string();

                if resp.status() != reqwest::StatusCode::OK {
                    bail!("Failed to fetch gem");
                }
                let text = resp.text().unwrap();
                let mut versions = text.lines().collect::<Vec<_>>();
                if let Some((idx, _)) = versions
                    .iter()
                    .enumerate()
                    .find(|(_, name)| **name == "---")
                {
                    versions = versions[idx + 1..].to_vec();
                } else {
                    bail!("Failed to find separator in info for {}", name);
                }
                let versions = versions
                    .iter()
                    .map(|line| parse_info_line(name, line).map(|gem| (gem.full_name.clone(), gem)))
                    .collect::<miette::Result<HashMap<_, _>>>()?;

                let mut namespace = Namespace {
                    name: name.to_string(),
                    info_checksum,
                    versions,
                };

                println!("Namespace: {}", namespace.name);
                if let Some(existing) = existing {
                    if existing.info_checksum != namespace.info_checksum {
                        println!(
                            "Checksum mismatch for {}: {} vs {}",
                            name, existing.info_checksum, namespace.info_checksum
                        );
                        namespace.merge(existing);
                    } else {
                        println!("No changes for {}", name);
                    }
                } else {
                    println!("New namespace: {}", name);
                    index.gems.insert(name.to_string(), namespace);
                }
            }

            let gems = &mut index.gems;

            for (_, gem) in gems.iter_mut() {
                for (_, version) in gem.versions.iter_mut() {
                    if version.stored {
                        println!("Already stored {}", version.full_name);
                        continue;
                    }

                    if !store.has_blob(&version.package_integrity) {
                        println!("Fetching blob for {}", version.full_name);
                        let blob_url = format!("{}/gems/{}.gem", index.source, version.full_name);
                        let resp = reqwest::blocking::get(&blob_url).unwrap();
                        if resp.status() != reqwest::StatusCode::OK {
                            bail!("Failed to fetch blob");
                        }
                        let blob = resp.bytes().unwrap();
                        let integrity = store.store_blob(&blob)?;
                        integrity.matches(&version.package_integrity).unwrap();
                    }

                    // Extract metadata from the blob

                    let dot_gem = store.get_blob(&version.package_integrity)?;
                    let mut archive = tar::Archive::new(dot_gem.as_slice());
                    let mut metadata = None;
                    for entry in archive.entries().unwrap() {
                        let mut entry = entry.unwrap();
                        if entry.path().unwrap().as_ref() == OsStr::new("metadata.gz") {
                            let mut buf = Vec::new();
                            entry.read_to_end(&mut buf).unwrap();
                            metadata = Some(buf);
                            break;
                        }
                    }
                    if metadata.is_none() {
                        bail!(
                            "Failed to find metadata.gz in blob for {}",
                            version.full_name
                        );
                    }
                    let metadata_gz_integrity = store.store_blob(metadata.unwrap())?;

                    version.metadata_gz_integrity = Some(metadata_gz_integrity.clone());
                    version.stored = true;
                }
            }
        }

        Ok(())
    })?;

    Ok(())
}

fn parse_info_line(name: &str, line: &str) -> miette::Result<Gem> {
    let (version, rest) = line
        .split_once(" ")
        .ok_or_else(|| miette!("Invalid line format"))?;

    let (_deps, metadata) = rest
        .split_once("|")
        .ok_or_else(|| miette!("Invalid line format"))?;

    let (version, platform) = version.split_once("-").unwrap_or((version, "ruby"));

    let full_name = if platform == "ruby" {
        format!("{}-{}", name, version)
    } else {
        format!("{}-{}-{}", name, version, platform)
    };

    let mut sha256 = String::new();
    metadata.split(',').for_each(|item| {
        if item.starts_with("checksum:") {
            sha256 = item.split(':').nth(1).unwrap_or("").to_string();
        }
    });

    Ok(Gem {
        full_name: full_name.to_string(),
        name: name.to_string(),
        version: version.to_string(),
        platform: platform.to_string(),
        package_integrity: Integrity::from_hex(sha256, ssri::Algorithm::Sha256).unwrap(),
        metadata_gz_integrity: None,
        stored: false,
    })
}

#[derive(Debug, Default)]
pub struct MemoryStore {
    indices: Vec<Index>,

    blobs: RwLock<RefCell<HashMap<String, Vec<u8>>>>,
}

impl Store for MemoryStore {
    fn list_indices(&self) -> miette::Result<Vec<Index>> {
        Ok(self.indices.clone())
    }

    fn store_blob<B: AsRef<[u8]>>(&self, blob: B) -> miette::Result<Integrity> {
        let integrity = Integrity::from(blob.as_ref());
        let blobs = self.blobs.read().unwrap();
        blobs
            .borrow_mut()
            .insert(integrity.to_string(), blob.as_ref().to_vec());
        Ok(integrity)
    }

    fn get_blob(&self, sha256: &Integrity) -> miette::Result<Vec<u8>> {
        let blobs = self.blobs.read().unwrap();
        if let Some(blob) = blobs.borrow().get(sha256.to_string().as_str()) {
            sha256.check(blob.as_slice()).unwrap();
            Ok(blob.clone())
        } else {
            bail!("Blob not found")
        }
    }

    fn has_blob(&self, sha256: &Integrity) -> bool {
        let blobs = self.blobs.read().unwrap();
        blobs.borrow().contains_key(sha256.to_string().as_str())
    }

    fn with_indices<F>(&mut self, f: F) -> miette::Result<Vec<Index>>
    where
        F: FnOnce(&mut Self, &mut Vec<Index>) -> miette::Result<()>,
    {
        let mut indices = std::mem::take(&mut self.indices);
        f(self, &mut indices)?;
        self.indices = indices;
        Ok(self.indices.clone())
    }
}

pub struct FsStore {
    root: PathBuf,
}

impl FsStore {
    pub fn new<P: AsRef<Path>>(root: P) -> miette::Result<Self> {
        let root = PathBuf::from(root.as_ref());
        std::fs::create_dir_all(&root)
            .map_err(|e| miette!("Failed to create directory {}: {}", root.display(), e))?;
        Ok(Self { root })
    }
}

impl Store for FsStore {
    fn list_indices(&self) -> miette::Result<Vec<Index>> {
        let path = self.root.join("indices.json");
        if !path.exists() {
            return Ok(vec![]);
        }
        let file = std::fs::File::open(path).unwrap();
        let reader = std::io::BufReader::new(file);
        let indices = serde_json::from_reader(reader).unwrap();
        Ok(indices)
    }

    fn store_blob<B: AsRef<[u8]>>(&self, blob: B) -> miette::Result<Integrity> {
        cacache::write_hash_sync(&self.root, blob)
            .map_err(|e| miette!("Failed to store blob: {}", e))
    }

    fn get_blob(&self, sha256: &Integrity) -> miette::Result<Vec<u8>> {
        cacache::read_hash_sync(&self.root, sha256)
            .map_err(|e| miette!("Failed to get blob: {}", e))
    }

    fn has_blob(&self, sha256: &Integrity) -> bool {
        cacache::exists_sync(&self.root, sha256)
    }

    fn with_indices<F>(&mut self, f: F) -> miette::Result<Vec<Index>>
    where
        F: FnOnce(&mut Self, &mut Vec<Index>) -> miette::Result<()>,
    {
        let mut indices = self.list_indices()?;
        f(self, &mut indices)?;
        let path = self.root.join("indices.json");
        let file =
            fs::File::create(&path).map_err(|e| miette!("Failed to open indices.json: {}", e))?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer(writer, &indices)
            .map_err(|e| miette!("Failed to write indices.json: {}", e))?;
        Ok(indices)
    }
}
