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
    pub time_stamp: Tm,
    pub air_temperature: f64,
    pub air_relative_humidity: f64,
    pub solar_radiation: f64,
    pub soil_water_content: f64,
    pub soil_temperature: f64,
    pub wind_speed: f64,
    pub wind_max: f64,
    pub wind_direction: f64,
    pub percipitation: f64,
    pub air_pressure: f64,
}

/// Wrapper type: do we have just battery data or everything else ?
#[derive(Debug, Clone, PartialEq)]
pub enum StationDataType {
    BatteryVoltage(Tm, f64),
    OtherData(WeatherStationData)
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

/// Parse all other data besides battery voltage
fn parse_other_data(time_stamp: &Tm, values: &Vec<&str>) -> Result<StationDataType, ParseError> {
    Err(ParseError::EmptyBuffer)
}

/// Parse all the data that is send from the weather station.
pub fn parse_data(buffer: &[u8]) -> Result<StationDataType, ParseError> {
    let line = str::from_utf8(buffer);

    match line {
        Ok(line_str) => {
            if line_str.is_empty() {
                Err(ParseError::EmptyBuffer)
            } else {
                //let line_string = line_str.as_string();
                let re = Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap();
                if re.is_match(line_str) {
                    // Found valid time stamp
                    let line_elements: Vec<&str> = line_str.split(',').collect();
                    // println!("time_stamp: {}", line_elements[0]);
                    // let remove_chars: &[_] = &['\x00', '\x02', '\x1E', '\x22', 'J'];
                    // println!("time_stamp_str: {}", time_stamp_str);
                    let time_stamp = strptime(line_elements[0].trim_matches(|c| c < '0' || c > '9'), "%Y-%m-%d %H:%M:%S").unwrap();

                    if line_elements.len() == 3 { // Only battery
                        let battery_voltage = line_elements[1].parse::<f64>();

                        match battery_voltage {
                            Ok(value) => {
                                Ok(StationDataType::BatteryVoltage(time_stamp, value))
                            },
                            Err(e) => {
                                Err(ParseError::ParseFloatError(e))
                            }
                        }
                    } else if line_elements.len() == 11 { // All data
                        parse_other_data(&time_stamp, &line_elements)
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

#[cfg(test)]
mod tests {
    use time::strptime;

    use super::{parse_data, StationDataType, ParseError};

    #[test]
    fn test_parse_data_empty() {
        let result = parse_data(&[]);
        assert_eq!(result, Err(ParseError::EmptyBuffer));
    }

    #[test]
    fn test_parse_data_header1() {
        let result = parse_data(&[65, 65, 65]);
        assert_eq!(result, Err(ParseError::NoTimeStamp));
    }

    #[test]
    fn test_parse_data_header2() { // CSV header, we don't need it
        let result = parse_data(&[2, 0, 97, 34, 84, 83, 34, 44, 34, 68, 101, 103, 32, 67, 34,
            44, 34, 37, 34, 44, 34, 87, 47, 109, 66, 50, 34, 44, 34, 109, 66, 51, 47, 109, 66,
            51, 34, 44, 34, 68, 101, 103, 32, 67, 34, 44, 34, 109, 101, 116, 101, 114, 115, 47,
            115, 101, 99, 111, 110, 100, 34, 44, 34, 109, 101, 116, 101, 114, 115, 47, 115, 101,
            99, 111, 110, 100, 34, 44, 34, 100, 101, 103, 114, 101, 101, 115, 34, 44, 34, 109,
            109, 34, 44, 34, 109, 98, 97, 114, 34, 10]);
        assert_eq!(result, Err(ParseError::NoTimeStamp));
    }

    #[test]
    fn test_parse_data_correct1() { // All data from the station
        let result = parse_data(&[2, 0, 74, 34, 50, 48, 49, 54, 45, 48, 54, 45, 49, 49, 32, 48,
            57, 58, 48, 48, 58, 48, 48, 34, 44, 55, 46, 53, 54, 44, 51, 50, 46, 50, 53, 44, 49,
            46, 51, 51, 51, 44, 48, 46, 48, 50, 50, 44, 49, 53, 46, 49, 56, 44, 48, 46, 55, 56,
            50, 44, 49, 46, 55, 53, 44, 50, 53, 54, 46, 55, 44, 48, 44, 57, 53, 49, 10]);
        assert_eq!(result, Err(ParseError::EmptyBuffer));
    }

    #[test]
    fn test_parse_data_correct2() { // Only battery data
        let result = parse_data(&[2, 0, 30, 34, 50, 48, 49, 54, 45, 48, 54, 45, 49, 50, 32, 48,
            48, 58, 48, 48, 58, 48, 48, 34, 44, 49, 50, 46, 55, 51, 44, 48, 10]);
        assert_eq!(result, Ok(StationDataType::BatteryVoltage(strptime("2016-06-12 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(), 12.73)));
    }

}
