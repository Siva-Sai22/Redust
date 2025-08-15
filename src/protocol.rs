pub fn parse_resp(input: &str) -> Result<(Vec<String>, usize), &str> {
    let mut current_pos = 0;

    // Find the end of the first line (array header)
    let array_header_end = match input.find("\r\n") {
        Some(pos) => pos,
        None => return Err("Incomplete array header"),
    };
    let array_header = &input[..array_header_end];
    current_pos += array_header_end + 2;

    if !array_header.starts_with('*') {
        return Err("Expected an array ('*')");
    }

    let num_elements: usize = array_header[1..]
        .parse()
        .map_err(|_| "Invalid array length")?;

    let mut result = Vec::with_capacity(num_elements);
    for _ in 0..num_elements {
        // Parse bulk string header
        let bulk_header_end = match input[current_pos..].find("\r\n") {
            Some(pos) => pos,
            None => return Err("Incomplete bulk string header"),
        };
        let bulk_header = &input[current_pos..current_pos + bulk_header_end];
        current_pos += bulk_header_end + 2;

        if !bulk_header.starts_with('$') {
            return Err("Expected a bulk string ('$')");
        }
        let bulk_len: usize = bulk_header[1..]
            .parse()
            .map_err(|_| "Invalid bulk string length")?;

        // Check if we have enough data for the bulk string content + CRLF
        if input.len() < current_pos + bulk_len + 2 {
            return Err("Incomplete bulk string content");
        }

        let string_content = &input[current_pos..current_pos + bulk_len];
        result.push(string_content.to_string());
        current_pos += bulk_len + 2; // +2 for the trailing \r\n
    }

    Ok((result, current_pos))
}

pub fn serialize_resp_array(items: &[String]) -> String {
    let mut resp = format!("*{}\r\n", items.len());
    for item in items {
        resp.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
    }
    resp
}