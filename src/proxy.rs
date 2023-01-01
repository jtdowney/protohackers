use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
};

use eyre::bail;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_till1},
    character::complete::{space1, u16 as nom_u16},
    combinator::map_res,
    Finish, IResult,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

fn ipv4_addr(input: &str) -> IResult<&str, IpAddr> {
    map_res(take_till1(|c: char| c.is_ascii_whitespace()), |addr| {
        Ipv4Addr::from_str(addr).map(IpAddr::from)
    })(input)
}

fn ipv6_addr(input: &str) -> IResult<&str, IpAddr> {
    map_res(take_till1(|c: char| c.is_ascii_whitespace()), |addr| {
        Ipv6Addr::from_str(addr).map(IpAddr::from)
    })(input)
}

fn addr<'a>(input: &'a str, protocol: &'a str) -> IResult<&'a str, IpAddr> {
    if protocol == "TCP4" {
        ipv4_addr(input)
    } else {
        ipv6_addr(input)
    }
}

fn proxy_proto(input: &str) -> IResult<&str, (SocketAddr, SocketAddr)> {
    let (input, _) = tag("PROXY")(input)?;
    let (input, _) = space1(input)?;
    let (input, protocol) = alt((tag("TCP4"), tag("TCP6")))(input)?;
    let (input, _) = space1(input)?;
    let (input, source_address) = addr(input, protocol)?;
    let (input, _) = space1(input)?;
    let (input, destination_address) = addr(input, protocol)?;
    let (input, _) = space1(input)?;
    let (input, source_port) = nom_u16(input)?;
    let (input, _) = space1(input)?;
    let (input, destination_port) = nom_u16(input)?;

    let source = SocketAddr::new(source_address, source_port);
    let destination = SocketAddr::new(destination_address, destination_port);

    Ok((input, (source, destination)))
}

pub async fn read_proxy_proto<R: AsyncBufRead + Unpin>(reader: &mut R) -> eyre::Result<SocketAddr> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    match proxy_proto(&line).finish() {
        Ok((_, (source, _))) => Ok(source),
        Err(e) => bail!("error reading proxy protocol header {:?}: {}", line, e),
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::*;

    #[test]
    fn tcp4_connection() -> eyre::Result<()> {
        let (_, (source, destination)) = proxy_proto("PROXY TCP4 1.2.3.4 5.6.7.8 45600 80")?;
        assert_eq!(source, "1.2.3.4:45600".parse()?);
        assert_eq!(destination, "5.6.7.8:80".parse()?);
        Ok(())
    }

    #[test]
    fn tcp6_connection() -> eyre::Result<()> {
        let (_, (source, destination)) =
            proxy_proto("PROXY TCP6 2001:db8::8a2e:370:7334 ::1 45600 80")?;
        assert_eq!(source, "[2001:db8::8a2e:370:7334]:45600".parse()?);
        assert_eq!(destination, "[::1]:80".parse()?);
        Ok(())
    }

    #[test]
    fn mismatched_source() -> eyre::Result<()> {
        let result = proxy_proto("PROXY TCP6 1.3.4.5 ::1 45600 80");
        assert!(matches!(
            result,
            Err(nom::Err::Error(nom::error::Error {
                input: "1.3.4.5 ::1 45600 80",
                ..
            }))
        ));
        Ok(())
    }

    #[test]
    fn mismatched_destination() -> eyre::Result<()> {
        let result = proxy_proto("PROXY TCP4 1.3.4.5 ::1 45600 80");
        assert!(matches!(
            result,
            Err(nom::Err::Error(nom::error::Error {
                input: "::1 45600 80",
                ..
            }))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn reading_header() -> eyre::Result<()> {
        let reader = tokio_test::io::Builder::new()
            .read(b"PROXY TCP4 1.2.3.4 5.6.7.8 45600 80")
            .build();
        let mut reader = BufReader::new(reader);

        let source = read_proxy_proto(&mut reader).await?;
        assert_eq!(source, "1.2.3.4:45600".parse()?);

        Ok(())
    }

    #[tokio::test]
    async fn error_reading_header() -> eyre::Result<()> {
        let reader = tokio_test::io::Builder::new().read(b"error").build();
        let mut reader = BufReader::new(reader);

        let result = read_proxy_proto(&mut reader).await.unwrap_err();
        assert_eq!(
            result.to_string(),
            "error reading proxy protocol header \"error\": error Tag at: error"
        );

        Ok(())
    }
}
