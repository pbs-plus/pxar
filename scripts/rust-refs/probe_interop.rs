use std::io::Read;

use pxar::decoder::sync::Decoder;
use pxar::encoder::sync::Encoder;
use pxar::format::{self, acl};
use pxar::{EntryKind, Metadata, MetadataBuilder, PxarVariant};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn ts(secs: i64, nanos: u32) -> format::StatxTimestamp {
    format::StatxTimestamp::new(secs, nanos)
}

fn root_meta() -> Metadata {
    MetadataBuilder::new(format::mode::IFDIR | 0o755)
        .owner(1000, 1000)
        .mtime_full(ts(0x11223344, 0x22334455))
        .xattr("user.root", "rootval")
        .build()
}

fn big_content() -> Vec<u8> {
    (0..70000usize).map(|i| ((i * 7 + 13) & 0xff) as u8).collect()
}

fn encode<T: pxar::encoder::SeqWrite>(encoder: &mut Encoder<T>) -> Result<(), Error> {
    let file_meta = MetadataBuilder::new(format::mode::IFREG | 0o644)
        .owner(0, 0)
        .mtime_full(ts(1234567890, 42))
        .xattr("user.zeta", "z")
        .xattr("user.alpha", &[0u8, 1, 2, 255][..])
        .fcaps(Some(vec![1, 0, 0, 2, 0x20, 0, 0, 0, 0x20, 0, 0, 0, 0, 0, 0, 0]))
        .build();
    let a_off = encoder.add_file(&file_meta, "a.txt", 10, &mut &b"hello pxar"[..])?;

    let empty_meta = MetadataBuilder::new(format::mode::IFREG | 0o600)
        .owner(1, 2)
        .mtime_full(ts(3, 4))
        .build();
    encoder.add_file(&empty_meta, "empty", 0, &mut &b""[..])?;

    let link_meta = MetadataBuilder::new(format::mode::IFLNK | 0o777)
        .owner(1000, 1000)
        .mtime_full(ts(5, 6))
        .build();
    encoder.add_symlink(&link_meta, "link", "a.txt")?;

    encoder.add_hardlink("hl", "a.txt", a_off)?;

    let cdev_meta = MetadataBuilder::new(format::mode::IFCHR | 0o666)
        .owner(0, 0)
        .mtime_full(ts(7, 8))
        .build();
    encoder.add_device(&cdev_meta, "cdev", format::Device { major: 1, minor: 3 })?;

    let bdev_meta = MetadataBuilder::new(format::mode::IFBLK | 0o660)
        .owner(0, 6)
        .mtime_full(ts(9, 10))
        .build();
    encoder.add_device(&bdev_meta, "bdev", format::Device { major: 7, minor: 0 })?;

    let fifo_meta = MetadataBuilder::new(format::mode::IFIFO | 0o644)
        .owner(11, 12)
        .mtime_full(ts(13, 14))
        .build();
    encoder.add_fifo(&fifo_meta, "fifo")?;

    let sock_meta = MetadataBuilder::new(format::mode::IFSOCK | 0o600)
        .owner(15, 16)
        .mtime_full(ts(17, 18))
        .build();
    encoder.add_socket(&sock_meta, "sock")?;

    let mut dir1_meta = MetadataBuilder::new(format::mode::IFDIR | 0o750)
        .owner(1000, 1000)
        .mtime_full(ts(19, 20))
        .acl_user(acl::User::new(1000, 7))
        .acl_user(acl::User::new(1001, 5))
        .acl_group(acl::Group::new(500, 4))
        .default_acl(Some(acl::Default {
            user_obj_permissions: acl::Permissions(7),
            group_obj_permissions: acl::Permissions(5),
            other_permissions: acl::Permissions(0),
            mask_permissions: acl::Permissions::NO_MASK,
        }))
        .default_acl_user(acl::User::new(2000, 6))
        .default_acl_group(acl::Group::new(3000, 4))
        .quota_project_id(Some(4711))
        .build();
    dir1_meta.acl.group_obj = Some(acl::GroupObject {
        permissions: acl::Permissions(6),
    });
    encoder.create_directory("dir1", &dir1_meta)?;
    let nested_meta = MetadataBuilder::new(format::mode::IFREG | 0o444)
        .owner(21, 22)
        .mtime_full(ts(23, 24))
        .build();
    encoder.add_file(&nested_meta, "nested.txt", 14, &mut &b"nested content"[..])?;
    encoder.finish()?;

    let many_meta = MetadataBuilder::new(format::mode::IFDIR | 0o755)
        .owner(0, 0)
        .mtime_full(ts(25, 26))
        .build();
    encoder.create_directory("many", &many_meta)?;
    for i in 0..20 {
        let name = format!("f{:02}", i);
        let meta = MetadataBuilder::new(format::mode::IFREG | 0o644)
            .owner(0, 0)
            .mtime_full(ts(100 + i, 0))
            .build();
        encoder.add_file(&meta, &name, name.len() as u64, &mut name.as_bytes())?;
    }
    let uni_meta = MetadataBuilder::new(format::mode::IFREG | 0o644)
        .owner(0, 0)
        .mtime_full(ts(200, 0))
        .build();
    encoder.add_file(&uni_meta, "\u{fc}b\u{ea}r-\u{f1}o", 4, &mut &b"unic"[..])?;
    encoder.finish()?;

    let big_meta = MetadataBuilder::new(format::mode::IFREG | 0o644)
        .owner(0, 0)
        .mtime_full(ts(300, 999999999))
        .build();
    let big = big_content();
    encoder.add_file(&big_meta, "big.bin", big.len() as u64, &mut &big[..])?;

    Ok(())
}

fn cmd_encode_v1(out: &str) -> Result<(), Error> {
    let meta = root_meta();
    let mut encoder = Encoder::create(out, &meta)?;
    encode(&mut encoder)?;
    encoder.finish()?;
    encoder.close()?;
    Ok(())
}

fn cmd_encode_v2(out: &str, payload_out: &str) -> Result<(), Error> {
    let meta = root_meta();
    let output = pxar::encoder::sync::StandardWriter::new(std::fs::File::create(out)?);
    let payload = pxar::encoder::sync::StandardWriter::new(std::fs::File::create(payload_out)?);
    let mut encoder = Encoder::new(
        PxarVariant::Split(output, payload),
        &meta,
        Some(b"prelude-blob-data"),
    )?;
    encode(&mut encoder)?;
    encoder.finish()?;
    encoder.close()?;
    Ok(())
}

fn dump_metadata(meta: &Metadata) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "mode={:o} uid={} gid={} mtime={}.{:09}",
        meta.stat.mode, meta.stat.uid, meta.stat.gid, meta.stat.mtime.secs, meta.stat.mtime.nanos
    ));
    for x in &meta.xattrs {
        s.push_str(&format!(
            " xattr[{}]={:x}",
            String::from_utf8_lossy(x.name().to_bytes()),
            fnv1a(x.value())
        ));
    }
    for u in &meta.acl.users {
        s.push_str(&format!(" acl_user[{}]={}", u.uid, u.permissions.0));
    }
    for g in &meta.acl.groups {
        s.push_str(&format!(" acl_group[{}]={}", g.gid, g.permissions.0));
    }
    if let Some(go) = &meta.acl.group_obj {
        s.push_str(&format!(" acl_group_obj={}", go.permissions.0));
    }
    if let Some(d) = &meta.acl.default {
        s.push_str(&format!(
            " acl_default={},{},{},{}",
            d.user_obj_permissions.0,
            d.group_obj_permissions.0,
            d.other_permissions.0,
            d.mask_permissions.0
        ));
    }
    for u in &meta.acl.default_users {
        s.push_str(&format!(" acl_default_user[{}]={}", u.uid, u.permissions.0));
    }
    for g in &meta.acl.default_groups {
        s.push_str(&format!(" acl_default_group[{}]={}", g.gid, g.permissions.0));
    }
    if let Some(f) = &meta.fcaps {
        s.push_str(&format!(" fcaps={:x}", fnv1a(&f.data)));
    }
    if let Some(q) = &meta.quota_project_id {
        s.push_str(&format!(" quota={}", q.projid));
    }
    s
}

fn cmd_decode(archive: &str, payload: Option<&str>) -> Result<(), Error> {
    let input = match payload {
        Some(p) => PxarVariant::Split(archive, p),
        None => PxarVariant::Unified(archive),
    };
    let mut decoder = Decoder::open(input)?;
    loop {
        let entry = match decoder.next() {
            None => break,
            Some(e) => e?,
        };
        let path = entry.path().to_string_lossy().into_owned();
        let kind = match entry.kind() {
            EntryKind::Version(v) => format!("version {:?}", v),
            EntryKind::Prelude(p) => format!("prelude {:x}", fnv1a(&p.data)),
            EntryKind::Symlink(l) => format!("symlink {}", l.as_os_str().to_string_lossy()),
            EntryKind::Hardlink(h) => {
                format!("hardlink {} off={}", h.as_os_str().to_string_lossy(), h.offset)
            }
            EntryKind::Device(d) => format!("device {}:{}", d.major, d.minor),
            EntryKind::Socket => "socket".to_string(),
            EntryKind::Fifo => "fifo".to_string(),
            EntryKind::File { size, .. } => {
                let mut buf = Vec::new();
                if let Some(mut c) = decoder.contents()? {
                    c.read_to_end(&mut buf)?;
                }
                format!("file size={} content={:x}", size, fnv1a(&buf))
            }
            EntryKind::Directory => "directory".to_string(),
            EntryKind::GoodbyeTable => "goodbye".to_string(),
        };
        match entry.kind() {
            EntryKind::Version(_) | EntryKind::Prelude(_) => {
                println!("{}", kind);
            }
            _ => {
                println!("{} :: {} :: {}", path, kind, dump_metadata(entry.metadata()));
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("encode-v1") => cmd_encode_v1(&args[2]),
        Some("encode-v2") => cmd_encode_v2(&args[2], &args[3]),
        Some("decode") => cmd_decode(&args[2], args.get(3).map(|s| s.as_str())),
        _ => Err("usage: probe_interop encode-v1|encode-v2|decode ...".into()),
    }
}
