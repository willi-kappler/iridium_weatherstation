//! Parse incoming data
//! Support for CSV and binary data

// System modules:
use std::str;
use std::num;
use std::io;
use std::io::Cursor;
use std::f64::{INFINITY, NEG_INFINITY, NAN};
use std::fs::File;
use std::io::Read;
use std::fmt;

// External modules:
use chrono::{NaiveDateTime};
use time::{Duration};
use regex::Regex;
use byteorder::{LittleEndian, BigEndian, ReadBytesExt};
use log::{info};

/// The actual data sent from each weather station
#[derive(Debug, Clone, PartialEq)]
pub struct WeatherStationData {
    pub timestamp: NaiveDateTime,
    pub air_temperature: f64,
    pub air_relative_humidity: f64,
    pub solar_radiation: f64,
    pub soil_water_content: f64,
    pub soil_temperature: f64,
    pub wind_speed: f64,
    pub wind_max: f64,
    pub wind_direction: f64,
    pub precipitation: f64,
    pub air_pressure: f64,
}

/// Wrapper type: do we have just battery data or everything else ?
#[derive(Debug, Clone, PartialEq)]
pub enum StationDataType {
    /// Simple data is just the time stamp, two battery voltage and wind.
    SimpleData(NaiveDateTime, f64, f64, f64),
    /// Multiple data contains the time stamp and all the other data values
    MultipleData(WeatherStationData)
}

/// ErrorType, what can go wrong during parsing...
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    EmptyBuffer,
    InvalidDataHeader,
    NoTimeStamp,
    WrongNumberOfColumns,
    ParseFloatError(num::ParseFloatError),
    Utf8Error(str::Utf8Error),
    IOError,
    ParseIntError(num::ParseIntError),
}

impl From<io::Error> for ParseError {
    fn from(_: io::Error) -> ParseError {
        ParseError::IOError
    }
}

impl From<num::ParseFloatError> for ParseError {
    fn from(err: num::ParseFloatError) -> ParseError {
        ParseError::ParseFloatError(err)
    }
}

impl From<str::Utf8Error> for ParseError {
    fn from(err: str::Utf8Error) -> ParseError {
        ParseError::Utf8Error(err)
    }
}

impl From<num::ParseIntError> for ParseError {
    fn from(err: num::ParseIntError) -> ParseError {
        ParseError::ParseIntError(err)
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EmptyBuffer => {
                write!(f, "Empty buffer")
            }
            ParseError::InvalidDataHeader => {
                write!(f, "Invalid data header")
            }
            ParseError::NoTimeStamp => {
                write!(f, "Invalid data: no time stamp found")
            }
            ParseError::WrongNumberOfColumns => {
                write!(f, "Invalid data: wrong number of columns (allowed: 3 or 11)")
            }
            ParseError::ParseFloatError(e) => {
                write!(f, "Parse float error: {}", e)
            }
            ParseError::Utf8Error(e) => {
                write!(f, "UFT8 error: {}", e)
            }
            ParseError::IOError => {
                write!(f, "IOError")
            }
            ParseError::ParseIntError(e) => {
                write!(f, "Parse int error: {}", e)
            }
        }
    }
}

/// Parse all other data besides battery voltage
fn parse_other_data(timestamp: &NaiveDateTime, line_elements: &Vec<&str>) -> Result<StationDataType, ParseError> {
    println!("line_elements: {:?}", line_elements);

    let air_temperature = line_elements[1].parse::<f64>()?;
    let air_relative_humidity = line_elements[2].parse::<f64>()?;
    let solar_radiation = line_elements[3].parse::<f64>()?;
    let soil_water_content = line_elements[4].parse::<f64>()?;
    let soil_temperature = line_elements[5].parse::<f64>()?;
    let wind_speed = line_elements[6].parse::<f64>()?;
    let wind_max = line_elements[7].parse::<f64>()?;
    let wind_direction = line_elements[8].parse::<f64>()?;
    let precipitation = line_elements[9].parse::<f64>()?;
    let air_pressure = line_elements[10].parse::<f64>()?;

    Ok(StationDataType::MultipleData(WeatherStationData{
        timestamp: *timestamp,
        air_temperature: air_temperature,
        air_relative_humidity: air_relative_humidity,
        solar_radiation: solar_radiation,
        soil_water_content: soil_water_content,
        soil_temperature: soil_temperature,
        wind_speed: wind_speed,
        wind_max: wind_max,
        wind_direction: wind_direction,
        precipitation: precipitation,
        air_pressure: air_pressure
    }))
}

/// Parse all the data that is send (as text) from the weather station.
pub fn parse_text_data(buffer: &[u8]) -> Result<StationDataType, ParseError> {
    let line = str::from_utf8(buffer);

    match line {
        Ok(line_str) => {
            if line_str.is_empty() {
                Err(ParseError::EmptyBuffer)
            } else {
                let re = Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap();
                if re.is_match(line_str) {// Found valid time stamp
                    // Prepare for parsing, split line at every ','
                    let remove_junk = |c| c < '0' || c > '9';
                    let line_elements: Vec<&str> = line_str.split(',').map(|elem| elem.trim_matches(&remove_junk)).collect();
                    let timestamp = NaiveDateTime::parse_from_str(line_elements[0].trim_matches(&remove_junk), "%Y-%m-%d %H:%M:%S").unwrap();

                    if line_elements.len() == 3 { // Only battery voltage
                        let battery_voltage = line_elements[1].parse::<f64>();

                        match battery_voltage {
                            Ok(value) => {
                                Ok(StationDataType::SimpleData(timestamp, value, 0.0, 0.0))
                            },
                            Err(e) => {
                                Err(ParseError::ParseFloatError(e))
                            }
                        }
                    } else if line_elements.len() == 11 { // All data
                        parse_other_data(&timestamp, &line_elements)
                    } else {
                        Err(ParseError::WrongNumberOfColumns)
                    }
                } else {
                    Err(ParseError::NoTimeStamp)
                }
            }
        },
        Err(e) => {
            Err(ParseError::Utf8Error(e))
        }
    }
}

fn u32_to_timestamp(seconds: u32) -> NaiveDateTime {
    let datetime_base = NaiveDateTime::parse_from_str("1990-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    datetime_base + Duration::seconds(seconds as i64)
}

fn u16_to_f64(data: u16) -> f64 {
    // base16 2 byte floats:
    // https://en.wikipedia.org/wiki/Half-precision_floating-point_format
    // https://github.com/sgothel/jogl/blob/master/src/jogl/classes/com/jogamp/opengl/math/Binary16.java
    // https://books.google.de/books?id=FPlICAAAQBAJ&pg=PA84&lpg=PA84&dq=binary16&source=bl&ots=0FAzD4XOqn&sig=98h_pzPlLzUXjB4uY1T8MRIZOnA&hl=de&sa=X&ved=0ahUKEwjkpvXU5ZzLAhVD9HIKHQOfAxYQ6AEITzAH#v=onepage&q=binary16&f=false
    // http://www.gamedev.net/topic/557338-ieee-754-2008-binary-16-inaccuracy-in-wikipedia/

    // Campbells own 2 bytes floating point format:
    // Bits: ABCDEFGH IJKLMNOP
    //
    // A: Sign, 0: +, 1: -
    //
    // B, C: Decimal position (exponent):
    // 0, 0: XXXX.
    // 0, 1: XXX.X
    // 1, 0: XX.XX
    // 1, 1: X.XXX
    //
    // D: being the MSB
    //
    // E-P: 13-bit binary value, Largest 13-bit magnitude (mantissa) is 8191, but Campbell Scientific defines the largest-allowable magnitude as 7999
    //
    // More information here:
    // https://www.campbellsci.com/forum?forum=1&l=thread&tid=540

    // 17660 = 252 + (68 * 256) = 01000100 11111100 -> 12.76
    // 17662 = 254 + (68 * 256) = 01000100 11111110 -> 12.78
    // 17664 = 69 * 256 =  01000101 00000000 -> 12.80
    // 24576 = (96 * 256) = 01100000 00000000 -> 0
    // 962 = 194 + (3 * 256) = 00000011 11000011 -> 963.0
    // 25576 = 232 + (99 * 256) = 01100011 11101000 -> 1.0

    const F2_POS_INFINITY: u16 = 0b00011111_11111111; // 31, 255
    const F2_NEG_INFINITY: u16 = 0b10011111_11111111; // 159, 255
    const F2_NAN: u16 = 0b10011111_11111110; // 159, 254

    if data == F2_POS_INFINITY {
        INFINITY
    } else if data == F2_NEG_INFINITY {
        NEG_INFINITY
    } else if data == F2_NAN {
        NAN
    } else {
        let sign = if data & 0b10000000_00000000 == 0 { 1.0 } else { - 1.0 };

        let mantissa: f64 = ((data & 0b00011111_11111111) as f64) * sign;
        let exponent: u16 = (data & 0b01100000_00000000) >> 13;

        match exponent {
            1 => mantissa / 10.0,
            2 => mantissa / 100.0,
            3 => mantissa / 1000.0,
            _ => mantissa
        }
    }
}

fn parse_binary_data_battery(buffer: &[u8]) -> Result<StationDataType, ParseError> {
    let mut read_bytes = Cursor::new(&buffer);

    // Time stamp
    let seconds = read_bytes.read_u32::<LittleEndian>()?;

    // Should be zero, not needed
    let _ = read_bytes.read_u32::<LittleEndian>()?;

    let solar_battery_voltage = read_bytes.read_u16::<BigEndian>()?;
    let lithium_battery_voltage = read_bytes.read_u16::<BigEndian>()?;
    let wind_diag = read_bytes.read_u16::<BigEndian>()?;

    Ok(StationDataType::SimpleData(u32_to_timestamp(seconds),
                                   u16_to_f64(solar_battery_voltage),
                                   u16_to_f64(lithium_battery_voltage),
                                   u16_to_f64(wind_diag)
                                   ))
}

fn parse_binary_data_multiple(buffer: &[u8]) -> Result<StationDataType, ParseError> {
    let mut read_bytes = Cursor::new(&buffer);

    // Time stamp
    let seconds = read_bytes.read_u32::<LittleEndian>()?;

    // Should be zero, not needed
    let _ = read_bytes.read_u32::<LittleEndian>()?;

    let air_temperature = read_bytes.read_u16::<BigEndian>()?;
    let air_relative_humidity = read_bytes.read_u16::<BigEndian>()?;
    let solar_radiation = read_bytes.read_u16::<BigEndian>()?;
    let soil_water_content = read_bytes.read_u16::<BigEndian>()?;
    let soil_temperature = read_bytes.read_u16::<BigEndian>()?;
    let wind_speed = read_bytes.read_u16::<BigEndian>()?;
    let wind_max = read_bytes.read_u16::<BigEndian>()?;
    let wind_direction = read_bytes.read_u16::<BigEndian>()?;
    let precipitation = read_bytes.read_u16::<BigEndian>()?;
    let air_pressure = read_bytes.read_u16::<BigEndian>()?;

    Ok(StationDataType::MultipleData(WeatherStationData{
        timestamp: u32_to_timestamp(seconds),
        air_temperature: u16_to_f64(air_temperature),
        air_relative_humidity: u16_to_f64(air_relative_humidity),
        solar_radiation: u16_to_f64(solar_radiation),
        soil_water_content: u16_to_f64(soil_water_content),
        soil_temperature: u16_to_f64(soil_temperature),
        wind_speed: u16_to_f64(wind_speed),
        wind_max: u16_to_f64(wind_max),
        wind_direction: u16_to_f64(wind_direction),
        precipitation: u16_to_f64(precipitation),
        air_pressure: u16_to_f64(air_pressure)
    }))
}

/// Parse all the data that is send (as binary) from the weather station.
pub fn parse_binary_data(buffer: &[u8]) -> Vec<Result<StationDataType, ParseError>> {
    const HEADER_LENGTH: u16 = 3;
    const ULONG_LEN: u16 = 4;
    const FP2_LEN: u16 = 2;

    const BATTERY_DATA_LENGTH: u16 = (2 * ULONG_LEN) + (3 * FP2_LEN);
    const FULL_DATA_LENGTH: u16 =  (2 * ULONG_LEN) + (10 * FP2_LEN);

    let mut result = Vec::new();

    if buffer.len() <= HEADER_LENGTH as usize {
        // Early return if buffer is too short
        result.push(Err(ParseError::EmptyBuffer))
    } else {
        if buffer[0] == 2 {
            let high = buffer[1] as u16;
            let low = buffer[2] as u16;
            let data_length = low + (256 * high);

            if (data_length as usize) != buffer.len() - 3 {
                info!("Data header incorrect, data_length: {}, actual length: {}", data_length, buffer.len() - 3)
            }

            if buffer.len() == (HEADER_LENGTH + BATTERY_DATA_LENGTH) as usize {
                // Looks like battery data
                result.push(parse_binary_data_battery(&buffer[3..]))
            } else if buffer.len() >= (HEADER_LENGTH + FULL_DATA_LENGTH) as usize {
                // Looks like multiple data
                for chunk in buffer[3..].chunks(FULL_DATA_LENGTH as usize) {
                    result.push(parse_binary_data_multiple(&chunk));
                }
            } else {
                result.push(Err(ParseError::InvalidDataHeader))
            }
        } else {
            result.push(Err(ParseError::InvalidDataHeader))
        }
    }

    result
}

fn open_and_read_file(filename: &str) -> Result<Vec<u8>, ParseError> {
    let mut f = File::open(filename)?;

    let mut whole_file = String::new();

    f.read_to_string(&mut whole_file)?;

    let mut result = Vec::new();

    for item in whole_file.split(',') {
        let value = item.trim().parse::<u8>()?;
        result.push(value);
    }

    Ok(result)
}

pub fn parse_binary_data_from_file(filename: &str) -> Vec<Result<StationDataType, ParseError>> {
    match open_and_read_file(filename) {
        Ok(data) => parse_binary_data(&data),
        Err(e) => vec![Err(e)]
    }
}

#[cfg(test)]
mod tests {
    use time::{Duration};
    use chrono::{NaiveDateTime};

    use super::*;
    use super::{u32_to_timestamp, u16_to_f64, parse_binary_data_battery, parse_binary_data_multiple, open_and_read_file};

    #[test]
    fn test_parse_text_data_empty() {
        let result = parse_text_data(&[]);
        assert_eq!(result, Err(ParseError::EmptyBuffer));
    }

    #[test]
    fn test_parse_text_data_header1() {
        let result = parse_text_data(&[65, 65, 65]);
        assert_eq!(result, Err(ParseError::NoTimeStamp));
    }

    #[test]
    fn test_parse_text_data_header2() { // CSV header, we don't need it
        let result = parse_text_data(&[2, 0, 97, 34, 84, 83, 34, 44, 34, 68, 101, 103, 32, 67, 34,
            44, 34, 37, 34, 44, 34, 87, 47, 109, 66, 50, 34, 44, 34, 109, 66, 51, 47, 109, 66,
            51, 34, 44, 34, 68, 101, 103, 32, 67, 34, 44, 34, 109, 101, 116, 101, 114, 115, 47,
            115, 101, 99, 111, 110, 100, 34, 44, 34, 109, 101, 116, 101, 114, 115, 47, 115, 101,
            99, 111, 110, 100, 34, 44, 34, 100, 101, 103, 114, 101, 101, 115, 34, 44, 34, 109,
            109, 34, 44, 34, 109, 98, 97, 114, 34, 10]);
        assert_eq!(result, Err(ParseError::NoTimeStamp));
    }

    #[test]
    fn test_parse_text_data_correct1() { // All data from the station
        let result = parse_text_data(&[2, 0, 74, 34, 50, 48, 49, 54, 45, 48, 54, 45, 49, 49, 32, 48,
            57, 58, 48, 48, 58, 48, 48, 34, 44, 55, 46, 53, 54, 44, 51, 50, 46, 50, 53, 44, 49,
            46, 51, 51, 51, 44, 48, 46, 48, 50, 50, 44, 49, 53, 46, 49, 56, 44, 48, 46, 55, 56,
            50, 44, 49, 46, 55, 53, 44, 50, 53, 54, 46, 55, 44, 48, 44, 57, 53, 49, 10]);
        assert_eq!(result, Ok(StationDataType::MultipleData(WeatherStationData{
            timestamp: NaiveDateTime::parse_from_str("2016-06-11 09:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            air_temperature: 7.56,
            air_relative_humidity: 32.25,
            solar_radiation: 1.333,
            soil_water_content: 0.022,
            soil_temperature: 15.18,
            wind_speed: 0.782,
            wind_max: 1.75,
            wind_direction: 256.7,
            precipitation: 0.0,
            air_pressure: 951.0
        })));
    }

    #[test]
    fn test_parse_text_data_correct2() { // Only battery data
        let result = parse_text_data(&[2, 0, 30, 34, 50, 48, 49, 54, 45, 48, 54, 45, 49, 50, 32, 48,
            48, 58, 48, 48, 58, 48, 48, 34, 44, 49, 50, 46, 55, 51, 44, 48, 10]);
        assert_eq!(result, Ok(StationDataType::SimpleData(NaiveDateTime::parse_from_str("2016-06-12 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(), 12.73, 0.0, 0.0)));
    }

    #[test]
    fn test_parse_text_data_wrong_columns() { // Wrong number of columns
        let result = parse_text_data(&[2, 0, 74, 34, 50, 48, 49, 54, 45, 48, 54, 45, 49, 49, 32, 48,
            57, 58, 48, 48, 58, 48, 48, 34, 44, 55, 46, 53, 54, 44, 51, 50, 46, 50, 53, 44, 49,
            46, 51, 51, 51, 44, 48, 46, 48, 50, 50, 44, 49, 53, 46, 49, 56, 44, 48, 46, 55]);
        assert_eq!(result, Err(ParseError::WrongNumberOfColumns));
    }

    #[test]
    fn test_u32_to_timestamp() {
        let result = u32_to_timestamp(843091200);
        let datetime = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(result, datetime + Duration::seconds(0));
    }

    #[test]
    fn test_u16_to_f64_1() {
        assert_eq!(u16_to_f64(17660), 12.76);
    }

    #[test]
    fn test_u16_to_f64_2() {
        assert_eq!(u16_to_f64(17662), 12.78);
    }

    #[test]
    fn test_u16_to_f64_3() {
        assert_eq!(u16_to_f64(17664), 12.80);
    }

    #[test]
    fn test_u16_to_f64_4() {
        assert_eq!(u16_to_f64(24576), 0.0);
    }

    #[test]
    fn test_u16_to_f64_5() {
        assert_eq!(u16_to_f64(962), 962.0);
    }

    #[test]
    fn test_u16_to_f64_6() {
        assert_eq!(u16_to_f64(25576), 1.0);
    }

    #[test]
    fn test_parse_binary_data_empty1() {
        let result = parse_binary_data(&[]);
        assert_eq!(result, vec![Err(ParseError::EmptyBuffer)]);
    }

    #[test]
    fn test_parse_binary_data_empty2() {
        let result = parse_binary_data(&[1]);
        assert_eq!(result, vec![Err(ParseError::EmptyBuffer)]);
    }

    #[test]
    fn test_parse_binary_data_empty3() {
        let result = parse_binary_data(&[1, 2]);
        assert_eq!(result, vec![Err(ParseError::EmptyBuffer)]);
    }

    #[test]
    fn test_parse_binary_data_empty4() {
        let result = parse_binary_data(&[1, 2, 3]);
        assert_eq!(result, vec![Err(ParseError::EmptyBuffer)]);
    }

    #[test]
    fn test_parse_binary_data_empty5() {
        let result = parse_binary_data(&[2, 1, 9, 0, 0, 0]);
        assert_eq!(result, vec![Err(ParseError::InvalidDataHeader)]);
    }

    #[test]
    fn test_parse_binary_data_empty6() {
        let result = parse_binary_data(&[2, 4, 167, 0, 0, 0]);
        assert_eq!(result, vec![Err(ParseError::InvalidDataHeader)]);
    }

    #[test]
    fn test_parse_binary_data_invalid_header() {
        let result = parse_binary_data(&[1, 2, 3, 4]);
        assert_eq!(result, vec![Err(ParseError::InvalidDataHeader)]);
    }

    #[test]
    fn test_parse_binary_data_battery() {
        let result = parse_binary_data_battery(&[0, 141, 64, 50, 0, 0, 0, 0, 68, 252, 96, 0, 0, 0]);
        let datetime = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap() + Duration::seconds(0);
        assert_eq!(result, Ok(StationDataType::SimpleData(datetime, 12.76, 0.0, 0.0)));
    }

    #[test]
    fn test_parse_binary_data_multiple() {
        let result = parse_binary_data_multiple(&[0, 141, 64, 50, 0, 0, 0, 0, 69, 222, 35, 229, 92, 249, 96, 77, 70, 100, 97, 103, 98, 238, 43, 190, 99, 232, 3, 194]);
        let datetime = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap() + Duration::seconds(0);
        assert_eq!(result, Ok(StationDataType::MultipleData(WeatherStationData{
            timestamp: datetime,
            air_temperature: 15.02,
            air_relative_humidity: 99.7,
            solar_radiation: 74.17,
            soil_water_content: 0.077,
            soil_temperature: 16.36,
            wind_speed: 0.359,
            wind_max: 0.75,
            wind_direction: 300.6,
            precipitation: 1.0,
            air_pressure: 962.0
        })));
    }

    #[test]
    fn test_parse_binary_data1() {
        let result = parse_binary_data(&[2, 0, 12, 0, 141, 64, 50, 0, 0, 0, 0, 68, 252, 96, 0, 0, 0]);
        let datetime = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap() + Duration::seconds(0);
        assert_eq!(result, vec![Ok(StationDataType::SimpleData(datetime, 12.76, 0.0, 0.0))]);
    }

    #[test]
    fn test_parse_binary_data2() {
        let result = parse_binary_data(&[2, 0, 28, 0, 141, 64, 50, 0, 0, 0, 0, 69, 222, 35, 229, 92, 249, 96, 77, 70, 100, 97, 103, 98, 238, 43, 190, 99, 232, 3, 194]);
        let datetime = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap() + Duration::seconds(0);
        assert_eq!(result, vec![Ok(StationDataType::MultipleData(WeatherStationData{
            timestamp: datetime,
            air_temperature: 15.02,
            air_relative_humidity: 99.7,
            solar_radiation: 74.17,
            soil_water_content: 0.077,
            soil_temperature: 16.36,
            wind_speed: 0.359,
            wind_max: 0.75,
            wind_direction: 300.6,
            precipitation: 1.0,
            air_pressure: 962.0
        }))]);
    }

    #[test]
    fn test_parse_binary_data3() {
        let result = parse_binary_data(&[2, 0, 28, 0, 141, 64, 50, 0, 0, 0, 0, 69, 222, 35, 229, 92, 249, 96, 77, 70, 100, 97, 103, 98, 238, 43, 190, 99, 232, 3, 194, 0]);
        let datetime = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap() + Duration::seconds(0);
        assert_eq!(result, vec![Ok(StationDataType::MultipleData(WeatherStationData{
            timestamp: datetime,
            air_temperature: 15.02,
            air_relative_humidity: 99.7,
            solar_radiation: 74.17,
            soil_water_content: 0.077,
            soil_temperature: 16.36,
            wind_speed: 0.359,
            wind_max: 0.75,
            wind_direction: 300.6,
            precipitation: 1.0,
            air_pressure: 962.0
        })), Err(ParseError::IOError)]);
    }

    #[test]
    fn test_open_and_read_file1() {
        let result = open_and_read_file("test_read_binary1.txt").unwrap();

        let expected = vec![2, 2, 160, 208, 232, 125, 50, 0, 0, 0, 0, 71, 214,
80, 198, 3, 236, 96, 210, 68, 33, 99, 52, 102, 74, 36, 81, 96, 0, 3, 114, 224, 246, 125, 50, 0, 0, 0, 0, 71, 250, 82, 136, 4, 14, 96, 210, 68, 37, 99,
55, 103, 8, 38, 64, 96, 0, 3, 113, 240, 4, 126, 50, 0, 0, 0, 0, 72, 53, 84, 50, 3, 239, 96, 209, 68, 51, 99, 194, 104, 52, 35, 239, 96, 0, 3, 113,
0, 19, 126, 50, 0, 0, 0, 0, 71, 243, 81, 13, 3, 148, 96, 209, 68, 76, 99, 165, 105, 46, 37, 133, 96, 0, 3, 113, 16, 33, 126, 50, 0, 0, 0, 0, 71, 226,
87, 89, 53, 112, 96, 209, 68, 113, 99, 115, 105, 166, 38, 78, 96, 0, 3, 112, 32, 47, 126, 50, 0, 0, 0, 0, 71, 124, 87, 93, 39, 53, 96, 208, 68,
158, 99, 180, 106, 210, 38, 222, 96, 0, 3, 112, 48, 61, 126, 50, 0, 0, 0, 0, 70, 254, 86, 203, 35, 121, 96, 208, 68, 204, 98, 211, 104, 112, 39, 103,
96, 0, 3, 112, 64, 75, 126, 50, 0, 0, 0, 0, 70, 17, 88, 33, 79, 30, 96, 208, 68, 248, 97, 248, 102, 74, 40, 23, 96, 0, 3, 112, 80, 89, 126, 50, 0,
 0, 0, 0, 68, 32, 93, 113, 115, 238, 96, 207, 69, 28, 98, 88, 100, 76, 40, 84, 96, 0, 3, 112, 96, 103, 126, 50, 0, 0, 0, 0, 67, 208, 87, 167, 96, 0,
 96, 207, 69, 48, 98, 74, 100, 136, 39, 177, 96, 0, 3, 112, 112, 117, 126, 50, 0, 0, 0, 0, 67, 204, 75, 89, 96, 0, 96, 207, 69, 63, 98, 193, 101, 100,
 39, 48, 96, 0, 3, 112, 128, 131, 126, 50, 0, 0, 0, 0, 67, 190, 70, 159, 96, 0, 96, 207, 69, 68, 98, 49, 100, 186, 39, 110, 96, 0, 3, 112, 144, 145,
 126, 50, 0, 0, 0, 0, 67, 192, 69, 220, 96, 0, 96, 206, 69, 64, 98, 93, 100, 26, 39, 214, 96, 0, 3, 112, 160, 159, 126, 50, 0, 0, 0, 0, 67, 160, 71,
 9, 96, 0, 96, 206, 69, 57, 98, 39, 103, 8, 39, 164, 96, 0, 3, 111, 176, 173, 126, 50, 0, 0, 0, 0, 67, 84, 74, 134, 96, 0, 96, 205, 69, 46, 98, 22,
 99, 62, 40, 55, 96, 0, 3, 111, 192, 187, 126, 50, 0, 0, 0, 0, 67, 82, 72, 39, 96, 0, 96, 205, 69, 33, 98, 69, 100, 56, 40, 4, 96, 0, 3, 111, 208,
201, 126, 50, 0, 0, 0, 0, 67, 186, 74, 219, 96, 0, 96, 205, 69, 15, 98, 71, 99, 242, 38, 230, 96, 0, 3, 110, 224, 215, 126, 50, 0, 0, 0, 0, 67, 202,
 80, 153, 96, 0, 96, 204, 69, 2, 97, 225, 100, 246, 38, 173, 96, 0, 3, 110, 240, 229, 126, 50, 0, 0, 0, 0, 125, 116, 83, 237, 107, 237, 96, 204, 68,
 241, 97, 214, 100, 36, 39, 253, 96, 0, 3, 110, 0, 244, 126, 50, 0, 0, 0, 0, 124, 204, 94, 167, 74, 146, 96, 204, 68, 223, 97, 116, 100, 16, 39, 113,
 96, 0, 3, 110, 16, 2, 127, 50, 0, 0, 0, 0, 68, 37, 71, 124, 35, 82, 96, 204, 68, 207, 97, 97, 101, 150, 40, 63, 96, 0, 3, 111, 32, 16, 127, 50, 0,
 0, 0, 0, 70, 140, 73, 150, 53, 117, 96, 204, 68, 190, 98, 54, 103, 218, 37, 253, 96, 0, 3, 111, 48, 30, 127, 50, 0, 0, 0, 0, 71, 159, 72, 65, 62, 79,
 96, 203, 68, 174, 98, 201, 103, 88, 40, 3, 96, 0, 3, 111, 64, 44, 127, 50, 0, 0, 0, 0, 71, 192, 74, 163, 3, 162, 96, 203, 68, 159, 99, 45, 104, 142,
 40, 31, 96, 0, 3, 111];

        assert_eq!(result, expected);
    }


    #[test]
    fn test_open_and_read_file2() {
        let result = open_and_read_file("test_read_binary2.txt").unwrap();

        let expected = vec![2, 0, 14, 128, 131, 126, 50, 0, 0, 0, 0, 69, 14, 109, 135, 96, 0];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_binary_data_from_file1() {
        let result = parse_binary_data_from_file("test_read_binary1.txt");

        assert_eq!(result.len(), 24);

        for val in result {
            assert!(val.is_ok());
        }
    }


    #[test]
    fn test_parse_binary_data_from_file2() {
        let result = parse_binary_data_from_file("test_read_binary2.txt");

        assert_eq!(result.len(), 1);
        assert!(result[0].is_ok());

        let data = result[0].as_ref().unwrap();

        assert_eq!(*data, StationDataType::SimpleData(
            NaiveDateTime::parse_from_str("2016-11-05 0:00:00", "%Y-%m-%d %H:%M:%S").unwrap() + Duration::seconds(0),
            12.94,
            3.463,
            0.0
        ));
    }

}
