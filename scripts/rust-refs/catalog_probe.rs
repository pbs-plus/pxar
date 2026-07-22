use std::io::Write;

const MAGIC: [u8; 8] = [145, 253, 96, 249, 196, 103, 88, 213];

fn enc_u64(buf: &mut Vec<u8>, mut d: u64) {
    let mut enc = Vec::new();
    loop {
        if d < 128 { enc.push(d as u8); break; }
        enc.push((128 | (d & 127)) as u8);
        d >>= 7;
    }
    buf.extend_from_slice(&enc);
}

fn enc_i64(buf: &mut Vec<u8>, v: i64) {
    let mut enc = Vec::new();
    let mut d = if v < 0 { (-1 * (v + 1)) as u64 + 1 } else { v as u64 };
    loop {
        if d < 128 {
            if v < 0 { enc.push(128 | d as u8); enc.push(0u8); }
            else { enc.push(d as u8); }
            break;
        }
        enc.push((128 | (d & 127)) as u8);
        d >>= 7;
    }
    buf.extend_from_slice(&enc);
}

const T_DIR: u8 = b'd';
const T_FILE: u8 = b'f';
const T_LINK: u8 = b'l';
const T_HARD: u8 = b'h';
const T_BLK: u8 = b'b';
const T_CHR: u8 = b'c';
const T_FIFO: u8 = b'p';
const T_SOCK: u8 = b's';

enum Attr {
    Dir { start: u64 },
    File { size: u64, mtime: i64 },
    Other,
}

struct Entry { name: Vec<u8>, attr: Attr }

struct DirInfo { name: Vec<u8>, entries: Vec<Entry> }

impl DirInfo {
    fn encode_entry(buf: &mut Vec<u8>, e: &Entry, start: u64) {
        match e.attr {
            Attr::Dir { start: child } => {
                buf.push(T_DIR); enc_u64(buf, e.name.len() as u64); buf.extend_from_slice(&e.name);
                enc_u64(buf, start - child);
            }
            Attr::File { size, mtime } => {
                buf.push(T_FILE); enc_u64(buf, e.name.len() as u64); buf.extend_from_slice(&e.name);
                enc_u64(buf, size); enc_i64(buf, mtime);
            }
            Attr::Other => {
                // type byte is set by caller via name convention; emulate all-other kinds:
                buf.push(T_LINK); enc_u64(buf, e.name.len() as u64); buf.extend_from_slice(&e.name);
            }
        }
    }
    fn encode(&self, start: u64) -> Vec<u8> {
        let mut table = Vec::new();
        enc_u64(&mut table, self.entries.len() as u64);
        for e in &self.entries {
            Self::encode_entry(&mut table, e, start);
        }
        let mut data = Vec::new();
        enc_u64(&mut data, table.len() as u64);
        data.extend_from_slice(&table);
        data
    }
}

fn main() {
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC);
    let mut pos = 8u64;

    // leaf: subdir
    let subdir = DirInfo { name: b"subdir".to_vec(), entries: vec![
        Entry { name: b"nested.txt".to_vec(), attr: Attr::File { size: 75, mtime: 2000 } },
    ]};
    let subdir_start = pos;
    let d = subdir.encode(subdir_start);
    pos += d.len() as u64;
    out.extend_from_slice(&d);

    // root entries (order matters; must match Go test)
    let root = DirInfo { name: b"".to_vec(), entries: vec![
        Entry { name: b"file1.txt".to_vec(), attr: Attr::File { size: 100, mtime: 1700000000 } },
        Entry { name: b"subdir".to_vec(), attr: Attr::Dir { start: subdir_start } },
        Entry { name: b"link".to_vec(), attr: Attr::Other },
        Entry { name: b"big".to_vec(), attr: Attr::File { size: 1u64 << 32, mtime: -1 } },
    ]};
    let root_start = pos;
    let d = root.encode(root_start);
    out.extend_from_slice(&d);

    out.write_all(&root_start.to_le_bytes()).unwrap();

    // emit hex to stdout
    for b in &out { print!("{:02x}", b); }
    println!();
}
