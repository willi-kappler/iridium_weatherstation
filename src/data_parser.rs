//! Parse incomming data
//! Currently text based CSV data
//! Will change to binary in the future

use std::str;
use std::num;
use time::{strptime, Tm};
use regex::Regex;

/// The actual data sent from each weather station
#[derive(Debug, Clone, PartialEq)]
pub struct WeatherStationData {
    pub timestamp: Tm,
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
    /// Single data is just the time stamp and the battery voltage
    SingleData(Tm, f64),
    /// Multipe data contains the time stamp and all the other data values
    MultipleData(WeatherStationData)
}

/// ErrorType, what can go wrong during parsing...
quick_error! {
    #[derive(Debug, Clone, PartialEq)]
    pub enum ParseError {
        EmptyBuffer {
            description("Empty buffer")
        }
        NoTimeStamp {
            description("Invalid data: no time stamp found")
        }
        WrongNumberOfColumns {
            description("Invalid data: wrong number of columns (allowed: 3 or 11)")
        }
        ParseFloatError(err: num::ParseFloatError) {
            description(err.description())
        }
        Utf8Error(err: str::Utf8Error) {
            description(err.description())
        }
    }
}

/// To be able to use the try! macro while parsin floating point values
impl From<num::ParseFloatError> for ParseError {
    fn from(err: num::ParseFloatError) -> ParseError {
        ParseError::ParseFloatError(err)
    }
}

/// Parse all other data besides battery voltage
fn parse_other_data(timestamp: &Tm, line_elements: &Vec<&str>) -> Result<StationDataType, ParseError> {
    println!("line_elements: {:?}", line_elements);

    let air_temperature = try!(line_elements[1].parse::<f64>());
    let air_relative_humidity = try!(line_elements[2].parse::<f64>());
    let solar_radiation = try!(line_elements[3].parse::<f64>());
    let soil_water_content = try!(line_elements[4].parse::<f64>());
    let soil_temperature = try!(line_elements[5].parse::<f64>());
    let wind_speed = try!(line_elements[6].parse::<f64>());
    let wind_max = try!(line_elements[7].parse::<f64>());
    let wind_direction = try!(line_elements[8].parse::<f64>());
    let precipitation = try!(line_elements[9].parse::<f64>());
    let air_pressure = try!(line_elements[10].parse::<f64>());

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
                    let timestamp = strptime(line_elements[0].trim_matches(&remove_junk), "%Y-%m-%d %H:%M:%S").unwrap();

                    if line_elements.len() == 3 { // Only battery voltage
                        let battery_voltage = line_elements[1].parse::<f64>();

                        match battery_voltage {
                            Ok(value) => {
                                Ok(StationDataType::SingleData(timestamp, value))
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

/// Parse all the data that is send (as binary) from the weather station.
pub fn parse_binary_data(buffer: &[u8]) -> Result<StationDataType, ParseError> {
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
    // B, C: Decimal position:
    // 0, 0: XXXX.
    // 0, 1: XXX.X
    // 1, 0: XX.XX
    // 1, 1: X.XXX
    //
    // D: being the MSB
    //
    // E-P: 13-bit binary value, Largest 13-bit magnitude is 8191, but Campbell Scientific defines the largest-allowable magnitude as 7999


    Err(ParseError::EmptyBuffer)
}

#[cfg(test)]
mod tests {
    use time::strptime;

    use super::{parse_text_data, StationDataType, ParseError, WeatherStationData};

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
            timestamp: strptime("2016-06-11 09:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
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
        assert_eq!(result, Ok(StationDataType::SingleData(strptime("2016-06-12 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(), 12.73)));
    }

    #[test]
    fn test_parse_text_data_wrong_columns() { // Wrong number of columns
        let result = parse_text_data(&[2, 0, 74, 34, 50, 48, 49, 54, 45, 48, 54, 45, 49, 49, 32, 48,
            57, 58, 48, 48, 58, 48, 48, 34, 44, 55, 46, 53, 54, 44, 51, 50, 46, 50, 53, 44, 49,
            46, 51, 51, 51, 44, 48, 46, 48, 50, 50, 44, 49, 53, 46, 49, 56, 44, 48, 46, 55]);
        assert_eq!(result, Err(ParseError::WrongNumberOfColumns));
    }

}
