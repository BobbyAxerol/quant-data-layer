const IV: [u64; 8] = [
    0x6a09e667f3bcc908,
    0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b,
    0xa54ff53a5f1d36f1,
    0x510e527fade682d1,
    0x9b05688c2b3e6c1f,
    0x1f83d9abfb41bd6b,
    0x5be0cd19137e2179,
];

const SIGMA: [[usize; 16]; 12] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
];

const PERSONAL: [u8; 16] = *b"qdl-event-v2\0\0\0\0";

pub fn deterministic_event_id(parts: &[&[u8]], size: usize) -> Result<Vec<u8>, String> {
    if !matches!(size, 16 | 32) {
        return Err("event ID size must be 16 or 32 bytes".into());
    }
    let mut input = Vec::new();
    for part in parts {
        let length = u32::try_from(part.len()).map_err(|_| "event ID part is too large")?;
        input.extend_from_slice(&length.to_be_bytes());
        input.extend_from_slice(part);
    }
    Ok(blake2b(&input, size))
}

fn blake2b(input: &[u8], digest_size: usize) -> Vec<u8> {
    let mut parameter = [0_u8; 64];
    parameter[0] = digest_size as u8;
    parameter[2] = 1;
    parameter[3] = 1;
    parameter[48..64].copy_from_slice(&PERSONAL);

    let mut state = IV;
    for (index, chunk) in parameter.chunks_exact(8).enumerate() {
        state[index] ^= u64::from_le_bytes(chunk.try_into().expect("fixed parameter chunk"));
    }

    if input.is_empty() {
        compress(&mut state, &[0_u8; 128], 0, true);
    } else {
        let chunks = input.chunks(128);
        let chunk_count = chunks.len();
        let mut consumed = 0_u128;
        for (index, chunk) in chunks.enumerate() {
            consumed += chunk.len() as u128;
            let mut block = [0_u8; 128];
            block[..chunk.len()].copy_from_slice(chunk);
            compress(&mut state, &block, consumed, index + 1 == chunk_count);
        }
    }

    state
        .iter()
        .flat_map(|word| word.to_le_bytes())
        .take(digest_size)
        .collect()
}

fn compress(state: &mut [u64; 8], block: &[u8; 128], counter: u128, last: bool) {
    let mut message = [0_u64; 16];
    for (index, chunk) in block.chunks_exact(8).enumerate() {
        message[index] = u64::from_le_bytes(chunk.try_into().expect("fixed message chunk"));
    }
    let mut work = [0_u64; 16];
    work[..8].copy_from_slice(state);
    work[8..].copy_from_slice(&IV);
    work[12] ^= counter as u64;
    work[13] ^= (counter >> 64) as u64;
    if last {
        work[14] = !work[14];
    }

    for schedule in SIGMA {
        mix(
            &mut work,
            0,
            4,
            8,
            12,
            message[schedule[0]],
            message[schedule[1]],
        );
        mix(
            &mut work,
            1,
            5,
            9,
            13,
            message[schedule[2]],
            message[schedule[3]],
        );
        mix(
            &mut work,
            2,
            6,
            10,
            14,
            message[schedule[4]],
            message[schedule[5]],
        );
        mix(
            &mut work,
            3,
            7,
            11,
            15,
            message[schedule[6]],
            message[schedule[7]],
        );
        mix(
            &mut work,
            0,
            5,
            10,
            15,
            message[schedule[8]],
            message[schedule[9]],
        );
        mix(
            &mut work,
            1,
            6,
            11,
            12,
            message[schedule[10]],
            message[schedule[11]],
        );
        mix(
            &mut work,
            2,
            7,
            8,
            13,
            message[schedule[12]],
            message[schedule[13]],
        );
        mix(
            &mut work,
            3,
            4,
            9,
            14,
            message[schedule[14]],
            message[schedule[15]],
        );
    }
    for index in 0..8 {
        state[index] ^= work[index] ^ work[index + 8];
    }
}

fn mix(work: &mut [u64; 16], a: usize, b: usize, c: usize, d: usize, x: u64, y: u64) {
    work[a] = work[a].wrapping_add(work[b]).wrapping_add(x);
    work[d] = (work[d] ^ work[a]).rotate_right(32);
    work[c] = work[c].wrapping_add(work[d]);
    work[b] = (work[b] ^ work[c]).rotate_right(24);
    work[a] = work[a].wrapping_add(work[b]).wrapping_add(y);
    work[d] = (work[d] ^ work[a]).rotate_right(16);
    work[c] = work[c].wrapping_add(work[d]);
    work[b] = (work[b] ^ work[c]).rotate_right(63);
}

#[cfg(test)]
mod tests {
    use super::deterministic_event_id;

    #[test]
    fn matches_python_personalized_blake2b_vector() {
        let actual = deterministic_event_id(&[b"AB", b"C", b"1"], 16).unwrap();
        assert_eq!(
            actual,
            [
                0x2b, 0xaf, 0xec, 0xbd, 0xcd, 0xdb, 0x40, 0x6a, 0x75, 0x44, 0xc0, 0x20, 0x8b, 0x2d,
                0x32, 0x0c,
            ]
        );
        assert_eq!(
            actual,
            deterministic_event_id(&[b"AB", b"C", b"1"], 16).unwrap()
        );
        assert_ne!(
            actual,
            deterministic_event_id(&[b"A", b"BC", b"1"], 16).unwrap()
        );
    }
}
