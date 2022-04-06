// iridium_weatherstation V0.3 (2022.04.05), written by Willi Kappler
//
// Licensed under the MIT License
//
// A simple data processing tool written in Rust for one of the campbell iridium weather stations
//

use std::net::{TcpStream, SocketAddr};
use std::io::{Read, Write, Cursor};
use std::fs::File;
use std::f64::{INFINITY, NEG_INFINITY, NAN};

use log::{info, debug, error};
use chrono::{Local, NaiveDateTime, Duration};
use byteorder::{LittleEndian, BigEndian, ReadBytesExt};

use crate::error::IWError;


pub const HEADER_LENGTH: usize = 48;
const HEADER_LENGTH2: usize = 3;
const ULONG_LEN: usize = 4;
const FP2_LEN: usize = 2;

const LOGGER_STATUS1_LENGTH: usize = (2 * ULONG_LEN) + (3 * FP2_LEN);
const LOGGER_STATUS2_LENGTH: usize = (3 * ULONG_LEN) + (3 * FP2_LEN);
const WEATHER_DATA_LENGTH: usize =  (2 * ULONG_LEN) + (10 * FP2_LEN);


// TODO: Read mapping from configuration file
fn port_to_station(port: u16) -> String{
    match port {
        2100 => "Nahuelbuta".to_string(),
        2101 => "Santa_Gracia".to_string(),
        2102 => "Pan_de_Azucar".to_string(),
        2103 => "La_Campana".to_string(),
        2104 => "Wanne_Tuebingen".to_string(),
        2001 => "test1".to_string(),
        2200 => "test2".to_string(),
        _ => "unknown".to_string()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct IWLoggerStatus {
    pub timestamp: NaiveDateTime,
    pub solar_battery: f64,
    pub lithium_battery: f64,
    pub wind_diag: f64,
    pub cf_card: u32,
}

#[derive(Clone, PartialEq, Debug)]
pub struct IWWeatherData {
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

#[derive(Clone, PartialEq, Debug)]
pub enum IWStationData {
    SingleData(IWLoggerStatus),
    MultipleData(Vec<IWWeatherData>),
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

fn parse_logger_status1(buffer: &[u8]) -> Result<IWStationData, IWError> {
    let mut read_bytes = Cursor::new(buffer);

    // Time stamp
    let seconds = read_bytes.read_u32::<LittleEndian>()?;

    // Should be zero, not needed
    let _ = read_bytes.read_u32::<LittleEndian>()?;

    let solar_battery_voltage = read_bytes.read_u16::<BigEndian>()?;
    let lithium_battery_voltage = read_bytes.read_u16::<BigEndian>()?;
    let wind_diag = read_bytes.read_u16::<BigEndian>()?;

    let result = IWLoggerStatus {
        timestamp: u32_to_timestamp(seconds),
        solar_battery: u16_to_f64(solar_battery_voltage),
        lithium_battery: u16_to_f64(lithium_battery_voltage),
        wind_diag: u16_to_f64(wind_diag),
        cf_card: 0,
    };

    Ok(IWStationData::SingleData(result))
}

fn parse_logger_status2(buffer: &[u8]) -> Result<IWStationData, IWError> {
    let mut read_bytes = Cursor::new(buffer);

    // Time stamp
    let seconds = read_bytes.read_u32::<LittleEndian>()?;

    // Should be zero, not needed
    let _ = read_bytes.read_u32::<LittleEndian>()?;

    let solar_battery_voltage = read_bytes.read_u16::<BigEndian>()?;
    let lithium_battery_voltage = read_bytes.read_u16::<BigEndian>()?;
    let wind_diag = read_bytes.read_u16::<BigEndian>()?;
    let cf_card = read_bytes.read_u32::<BigEndian>()?;

    let result = IWLoggerStatus {
        timestamp: u32_to_timestamp(seconds),
        solar_battery: u16_to_f64(solar_battery_voltage),
        lithium_battery: u16_to_f64(lithium_battery_voltage),
        wind_diag: u16_to_f64(wind_diag),
        cf_card,
    };

    Ok(IWStationData::SingleData(result))
}

fn parse_weather_data_single(buffer: &[u8]) -> Result<IWWeatherData, IWError> {
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

    let result = IWWeatherData {
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
        air_pressure: u16_to_f64(air_pressure),
    };

    Ok(result)
}

fn parse_weather_data(buffer: &[u8]) -> Result<IWStationData, IWError> {
    let mut result = Vec::new();

    for chunk in buffer.chunks(WEATHER_DATA_LENGTH) {
        result.push(parse_weather_data_single(chunk)?);
    }

    Ok(IWStationData::MultipleData(result))
}

fn get_data_length(buffer: &[u8]) -> usize {
    let high = buffer[1] as u16;
    let low = buffer[2] as u16;
    (low + (256 * high)) as usize
}

fn parse_binary_data(buffer: &[u8]) -> Result<IWStationData, IWError> {
    debug!("Parse binary data");

    let buffer_len = buffer.len();
    debug!("buffer_len: '{}'", buffer_len);

    if buffer_len < LOGGER_STATUS1_LENGTH {
        return Err(IWError::DataTooShort(buffer_len))
    }

    let data_len = get_data_length(buffer);
    debug!("data_len: '{}'", data_len);

    if data_len != buffer_len - HEADER_LENGTH2 {
        return Err(IWError::DataLengthMismatch(data_len))
    }

    if buffer[0] != 2 {
        return Err(IWError::InvalidDataHeader)
    }

    let data_buffer = &buffer[HEADER_LENGTH2..];

    if data_len == LOGGER_STATUS1_LENGTH {
        parse_logger_status1(data_buffer)
    } else if data_len == LOGGER_STATUS2_LENGTH {
        parse_logger_status2(data_buffer)
    } else {
        parse_weather_data(data_buffer)
    }
}

pub fn handle_connection(mut stream: TcpStream, socket: SocketAddr) -> Result<(), IWError> {
    debug!("New connection from '{}'", socket);

    let port = stream.local_addr()?.port();
    let station_name = port_to_station(port);
    debug!("Port: '{}', station: '{}'", port, station_name);

    let mut tcp_buffer = Vec::new();
    let len = stream.read_to_end(&mut tcp_buffer)?;
    debug!("[{}], number of bytes received: '{}'", port, len);

    if len < HEADER_LENGTH {
        return Err(IWError::DataTooShort(len))
    }

    let date_today = Local::now().format("%Y_%m_%d").to_string();

    // Write received binary data to disk.
    // Close binary file directly after this block.
    {
        let binary_filename = format!("old/binary/{}_{}.dat", station_name, date_today);
        let mut binary_file = File::create(&binary_filename)?;
        binary_file.write(&tcp_buffer)?;
        info!("Binary data written to: '{}'", binary_filename);
    }

    let after_header = &tcp_buffer[HEADER_LENGTH..];

    debug!("[{}] Binary data: {:?}", port, after_header);

    match parse_binary_data(after_header) {
        Ok(data) => {
            // Export data as CSV and as JSON
            match data {
                IWStationData::SingleData(data) => {
                    todo!();
                }
                IWStationData::MultipleData(data) => {
                    todo!();
                }
            }
        }
        Err(e) => {
            error!("An error occurred while parsing the data: '{}'", e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDateTime};

    use super::{u32_to_timestamp, u16_to_f64, parse_logger_status1, parse_logger_status2,
        parse_weather_data_single, parse_weather_data, get_data_length, parse_binary_data,
        IWStationData, IWLoggerStatus, IWWeatherData};

    use crate::error::IWError;

    #[test]
    fn test_u32_to_timestamp() {
        let result = u32_to_timestamp(843091200);
        let datetime = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(result, datetime);
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
    fn test_get_data_length1() {
        assert_eq!(get_data_length(&[0, 0, 0]), 0);
    }

    #[test]
    fn test_get_data_length2() {
        assert_eq!(get_data_length(&[0, 0, 27]), 27);
    }

    #[test]
    fn test_get_data_length3() {
        assert_eq!(get_data_length(&[0, 1, 0]), 256);
    }

    #[test]
    fn test_get_data_length4() {
        assert_eq!(get_data_length(&[0, 1, 4]), 260);
    }

    #[test]
    fn test_parse_logger_status1() {
        let result = parse_logger_status1(&[0, 141, 64, 50, 0, 0, 0, 0, 68, 252, 99, 240, 99, 220]).unwrap();
        let timestamp = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let expected = IWLoggerStatus {
            timestamp,
            solar_battery: 12.76,
            lithium_battery: 1.008,
            wind_diag: 0.988,
            cf_card: 0,
        };

        assert_eq!(result, IWStationData::SingleData(expected));
    }

    #[test]
    fn test_parse_logger_status2() {
        let result = parse_logger_status2(&[0, 141, 64, 50, 0, 0, 0, 0, 68, 252, 109, 31, 96, 0, 255, 255, 255, 127]).unwrap();
        let timestamp = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let expected = IWLoggerStatus {
            timestamp,
            solar_battery: 12.76,
            lithium_battery: 3.359,
            wind_diag: 0.0,
            cf_card: 4294967167,
        };

        assert_eq!(result, IWStationData::SingleData(expected));
    }

    #[test]
    fn test_parse_logger_status1_error() {
        let result = parse_logger_status1(&[0]);

        match result {
            Err(IWError::IO(_)) => {
                // OK
            }
            _ => {
                panic!("Expected IWError, got: '{:?}'", result);
            }
        }
    }

    #[test]
    fn test_parse_logger_status2_error() {
        let result = parse_logger_status2(&[0]);

        match result {
            Err(IWError::IO(_)) => {
                // OK
            }
            _ => {
                panic!("Expected IWError, got: '{:?}'", result);
            }
        }
    }

    #[test]
    fn test_parse_weather_data_single() {
        let result = parse_weather_data_single(&[0, 141, 64, 50, 0, 0, 0, 0, 69, 222, 35, 229, 92, 249, 96, 77, 70, 100, 97, 103, 98, 238, 43, 190, 99, 232, 3, 194]).unwrap();
        let timestamp = NaiveDateTime::parse_from_str("2016-09-19 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let expected = IWWeatherData {
            timestamp,
            air_temperature: 15.02,
            air_relative_humidity: 99.7,
            solar_radiation: 74.17,
            soil_water_content: 0.077,
            soil_temperature: 16.36,
            wind_speed: 0.359,
            wind_max: 0.75,
            wind_direction: 300.6,
            precipitation: 1.0,
            air_pressure: 962.0,
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_weather_data_single_error() {
        let result = parse_weather_data_single(&[0]);

        match result {
            Err(IWError::IO(_)) => {
                // OK
            }
            _ => {
                panic!("Expected IWError, got: '{:?}'", result);
            }
        }
    }

    #[test]
    fn test_parse_weather_data() {
        let result = parse_weather_data(&[
            208, 252, 170, 60, 0, 0, 0, 0, 70, 121, 93, 234, 3, 52, 96, 48, 72, 12, 119, 158, 67, 59, 42, 25, 96, 0, 3, 210,
            224, 10, 171, 60, 0, 0, 0, 0, 70, 146, 92, 255, 3, 108, 96, 48, 72, 12, 120, 106, 67, 66, 42, 30, 96, 0, 3, 210]).unwrap();

        let timestamp1 = NaiveDateTime::parse_from_str("2022-04-03 13:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let timestamp2 = NaiveDateTime::parse_from_str("2022-04-03 14:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

        let data1 = IWWeatherData {
            timestamp: timestamp1,
            air_temperature: 16.57,
            air_relative_humidity: 76.58,
            solar_radiation: 820.0,
            soil_water_content: 0.048,
            soil_temperature: 20.6,
            wind_speed: 6.046,
            wind_max: 8.27,
            wind_direction: 258.5,
            precipitation: 0.0,
            air_pressure: 978.0,
        };

        let data2 = IWWeatherData {
            timestamp: timestamp2,
            air_temperature: 16.82,
            air_relative_humidity: 74.23,
            solar_radiation: 876.0,
            soil_water_content: 0.048,
            soil_temperature: 20.6,
            wind_speed: 6.25,
            wind_max: 8.34,
            wind_direction: 259.0,
            precipitation: 0.0,
            air_pressure: 978.0,
        };

        let combined = IWStationData::MultipleData(vec![data1, data2]);

        assert_eq!(result, combined);
    }

    #[test]
    fn test_parse_weather_data_error() {
        let result = parse_weather_data(&[0]);

        match result {
            Err(IWError::IO(_)) => {
                // OK
            }
            _ => {
                panic!("Expected IWError, got: '{:?}'", result);
            }
        }
    }

    #[test]
    fn test_parse_binary_data1() {
        let result = parse_binary_data(&[2, 0, 14, 128, 151, 171, 60, 0, 0, 0, 0, 68, 209, 109, 116, 96, 0]).unwrap();

        let timestamp1 = NaiveDateTime::parse_from_str("2022-04-04 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

        let data1 = IWLoggerStatus {
            timestamp: timestamp1,
            solar_battery: 12.33,
            lithium_battery: 3.444,
            wind_diag: 0.0,
            cf_card: 0,
        };

        let data2 = IWStationData::SingleData(data1);

        assert_eq!(result, data2);
    }

    #[test]
    fn test_parse_binary_data2() {
        let result = parse_binary_data(&[2, 0, 18, 0, 233, 172, 60, 0, 0, 0, 0, 68, 223, 109, 41, 96, 0, 255, 255, 255, 127]).unwrap();

        let timestamp1 = NaiveDateTime::parse_from_str("2022-04-05 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

        let data1 = IWLoggerStatus {
            timestamp: timestamp1,
            solar_battery: 12.47,
            lithium_battery: 3.369,
            wind_diag: 0.0,
            cf_card: 4294967167,
        };

        let data2 = IWStationData::SingleData(data1);

        assert_eq!(result, data2);
    }

    #[test]
    fn test_parse_binary_data3() {
        let result = parse_binary_data(&[2, 0, 28, 208, 252, 170, 60, 0, 0, 0, 0, 70, 121, 93, 234, 3, 52, 96, 48, 72, 12, 119, 158, 67, 59, 42, 25, 96, 0, 3, 210]).unwrap();

        let timestamp1 = NaiveDateTime::parse_from_str("2022-04-03 13:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

        let data1 = IWWeatherData {
            timestamp: timestamp1,
            air_temperature: 16.57,
            air_relative_humidity: 76.58,
            solar_radiation: 820.0,
            soil_water_content: 0.048,
            soil_temperature: 20.6,
            wind_speed: 6.046,
            wind_max: 8.27,
            wind_direction: 258.5,
            precipitation: 0.0,
            air_pressure: 978.0,
        };

        let data2 = IWStationData::MultipleData(vec![data1]);

        assert_eq!(result, data2);
    }

    #[test]
    fn test_parse_binary_data_error1() {
        let result = parse_binary_data(&[0]);

        match result {
            Err(IWError::DataTooShort(1)) => {
                // OK
            }
            _ => {
                panic!("Expected IWError, got: '{:?}'", result);
            }
        }
    }

    #[test]
    fn test_parse_binary_data_error2() {
        let result = parse_binary_data(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

        match result {
            Err(IWError::DataLengthMismatch(0)) => {
                // OK
            }
            _ => {
                panic!("Expected IWError, got: '{:?}'", result);
            }
        }
    }

    #[test]
    fn test_parse_binary_data_error3() {
        let result = parse_binary_data(&[0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

        match result {
            Err(IWError::InvalidDataHeader) => {
                // OK
            }
            _ => {
                panic!("Expected IWError, got: '{:?}'", result);
            }
        }
    }
}
