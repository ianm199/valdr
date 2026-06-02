use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
struct Config {
    commands: usize,
    batches: Vec<usize>,
    frame: FrameKind,
    payload_bytes: usize,
    tmp_dir: PathBuf,
    keep_files: bool,
}

#[derive(Debug, Clone, Copy)]
enum FrameKind {
    Set,
    Incr,
}

#[derive(Debug)]
struct Row {
    variant: &'static str,
    frame: &'static str,
    commands: usize,
    batch_size: usize,
    write_calls: usize,
    sync_calls: usize,
    bytes: usize,
    elapsed: Duration,
    checksum: u64,
}

fn main() -> io::Result<()> {
    let cfg = parse_args();
    println!(
        "variant\tframe\tcommands\tbatch_size\twrite_calls\tsync_calls\tbytes\telapsed_ms\tcommands_per_s\tsyncs_per_command\tchecksum"
    );

    for &batch_size in &cfg.batches {
        if batch_size == 1 {
            emit(run_per_command(&cfg)?);
        }
        emit(run_batched(&cfg, batch_size)?);
    }
    Ok(())
}

fn run_per_command(cfg: &Config) -> io::Result<Row> {
    let path = output_path(cfg, "per-command", 1);
    let file = open_output(&path)?;
    let mut writer = BufWriter::new(file);
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    let start = Instant::now();
    for id in 0..cfg.commands {
        let frame = encode_frame(cfg, id);
        bytes += frame.len();
        checksum = checksum.wrapping_add(frame_checksum(&frame));
        writer.write_all(&frame)?;
        writer.flush()?;
        writer.get_ref().sync_data()?;
    }
    let elapsed = start.elapsed();
    cleanup(cfg, path);
    Ok(Row {
        variant: "per_command",
        frame: cfg.frame.as_str(),
        commands: cfg.commands,
        batch_size: 1,
        write_calls: cfg.commands,
        sync_calls: cfg.commands,
        bytes,
        elapsed,
        checksum,
    })
}

fn run_batched(cfg: &Config, batch_size: usize) -> io::Result<Row> {
    let batch_size = batch_size.max(1);
    let path = output_path(cfg, "batched", batch_size);
    let file = open_output(&path)?;
    let mut writer = BufWriter::new(file);
    let mut staged = Vec::with_capacity(batch_size.saturating_mul(128));
    let mut write_calls = 0usize;
    let mut sync_calls = 0usize;
    let mut bytes = 0usize;
    let mut checksum = 0u64;
    let start = Instant::now();

    for id in 0..cfg.commands {
        let frame = encode_frame(cfg, id);
        bytes += frame.len();
        checksum = checksum.wrapping_add(frame_checksum(&frame));
        staged.extend_from_slice(&frame);
        if (id + 1) % batch_size == 0 {
            flush_staged(&mut writer, &mut staged, &mut write_calls, &mut sync_calls)?;
        }
    }
    flush_staged(&mut writer, &mut staged, &mut write_calls, &mut sync_calls)?;

    let elapsed = start.elapsed();
    cleanup(cfg, path);
    Ok(Row {
        variant: "batched",
        frame: cfg.frame.as_str(),
        commands: cfg.commands,
        batch_size,
        write_calls,
        sync_calls,
        bytes,
        elapsed,
        checksum,
    })
}

fn flush_staged(
    writer: &mut BufWriter<File>,
    staged: &mut Vec<u8>,
    write_calls: &mut usize,
    sync_calls: &mut usize,
) -> io::Result<()> {
    if staged.is_empty() {
        return Ok(());
    }
    writer.write_all(staged)?;
    writer.flush()?;
    writer.get_ref().sync_data()?;
    *write_calls += 1;
    *sync_calls += 1;
    staged.clear();
    Ok(())
}

fn emit(row: Row) {
    let elapsed_ms = row.elapsed.as_secs_f64() * 1000.0;
    let commands_per_s = row.commands as f64 / row.elapsed.as_secs_f64().max(f64::EPSILON);
    let syncs_per_command = row.sync_calls as f64 / row.commands.max(1) as f64;
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{:.3}\t{:.6}\t{}",
        row.variant,
        row.frame,
        row.commands,
        row.batch_size,
        row.write_calls,
        row.sync_calls,
        row.bytes,
        elapsed_ms,
        commands_per_s,
        syncs_per_command,
        row.checksum
    );
}

fn encode_frame(cfg: &Config, id: usize) -> Vec<u8> {
    match cfg.frame {
        FrameKind::Set => {
            let key = format!("k:{id:08}");
            let value = value_payload(cfg.payload_bytes, id);
            encode_resp(&[b"SET".as_slice(), key.as_bytes(), value.as_slice()])
        }
        FrameKind::Incr => {
            let key = format!("counter:{:08}", id % 10_000);
            encode_resp(&[b"INCR".as_slice(), key.as_bytes()])
        }
    }
}

fn encode_resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.extend_from_slice(b"*");
    out.extend_from_slice(parts.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for part in parts {
        out.extend_from_slice(b"$");
        out.extend_from_slice(part.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn value_payload(len: usize, id: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    for (idx, byte) in out.iter_mut().enumerate() {
        *byte = b'a'.wrapping_add(((id + idx) % 26) as u8);
    }
    out
}

fn frame_checksum(frame: &[u8]) -> u64 {
    frame.iter().fold(0u64, |sum, byte| {
        sum.wrapping_mul(131).wrapping_add(*byte as u64)
    })
}

fn open_output(path: &PathBuf) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
}

fn output_path(cfg: &Config, variant: &str, batch_size: usize) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    cfg.tmp_dir.join(format!(
        "aof-batch-model-{variant}-{}-{batch_size}-{}-{stamp}.aof",
        cfg.frame.as_str(),
        std::process::id()
    ))
}

fn cleanup(cfg: &Config, path: PathBuf) {
    if !cfg.keep_files {
        let _ = std::fs::remove_file(path);
    }
}

impl FrameKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Incr => "incr",
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            commands: 2_000,
            batches: vec![1, 4, 16, 64, 256],
            frame: FrameKind::Set,
            payload_bytes: 64,
            tmp_dir: env::temp_dir(),
            keep_files: false,
        }
    }
}

fn parse_args() -> Config {
    let mut cfg = Config::default();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--commands" => {
                cfg.commands = parse_usize(args.next(), "--commands");
            }
            "--batches" => {
                cfg.batches = parse_batches(args.next());
            }
            "--frame" => {
                let value = args.next().unwrap_or_else(|| die("--frame requires a value"));
                cfg.frame = match value.as_str() {
                    "set" => FrameKind::Set,
                    "incr" => FrameKind::Incr,
                    _ => die("--frame must be set or incr"),
                };
            }
            "--payload-bytes" => {
                cfg.payload_bytes = parse_usize(args.next(), "--payload-bytes");
            }
            "--tmp-dir" => {
                cfg.tmp_dir = PathBuf::from(args.next().unwrap_or_else(|| die("--tmp-dir requires a value")));
            }
            "--keep-files" => {
                cfg.keep_files = true;
            }
            "-h" | "--help" => {
                print_help_and_exit();
            }
            _ => die(&format!("unknown argument: {arg}")),
        }
    }
    cfg.batches.sort_unstable();
    cfg.batches.dedup();
    if !cfg.batches.contains(&1) {
        cfg.batches.insert(0, 1);
    }
    cfg
}

fn parse_usize(value: Option<String>, name: &str) -> usize {
    value
        .unwrap_or_else(|| die(&format!("{name} requires a value")))
        .parse::<usize>()
        .unwrap_or_else(|_| die(&format!("{name} must be a non-negative integer")))
}

fn parse_batches(value: Option<String>) -> Vec<usize> {
    value
        .unwrap_or_else(|| die("--batches requires a value"))
        .split(',')
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<usize>()
                .unwrap_or_else(|_| die("--batches must be comma-separated integers"))
        })
        .filter(|n| *n > 0)
        .collect()
}

fn print_help_and_exit() -> ! {
    println!(
        "Usage: aof-batch-model [--commands N] [--frame set|incr] [--batches 1,4,16] [--payload-bytes N] [--tmp-dir PATH] [--keep-files]"
    );
    std::process::exit(0);
}

fn die(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}
