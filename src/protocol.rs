pub fn parse_resp(input: &str) -> Result<Vec<String>, &str> {
    let mut lines = input.split("\r\n").filter(|s| !s.is_empty());

    let array_header = lines.next().ok_or("Input is empty!")?;
    if !array_header.starts_with('*') {
        return Err("Expected an array ('*')");
    }

    let num_elements: usize = array_header[1..]
        .parse()
        .map_err(|_| "Invalid array length")?;

    let mut result = Vec::with_capacity(num_elements);
    for _ in 0..num_elements {
        let bulk_header = lines
            .next()
            .ok_or("Unexpected end of input while expecting bulk string header")?;
        if !bulk_header.starts_with('$') {
            return Err("Expected a bulk string ('$')");
        }
        let string_content = lines
            .next()
            .ok_or("Unexpected end of input while expecting string content")?;
        result.push(string_content.to_string());
    }

    Ok(result)
}