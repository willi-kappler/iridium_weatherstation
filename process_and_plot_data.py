#!/usr/bin/env python3

# This Python script plots the data from the weather stations in Chile.
# Written by Willi Kappler (willi.kappler@uni-tuebingen.de)
# Version V0.4 (2022.04.6)
# ESD - Earth System Dynamics:
# http://www.geo.uni-tuebingen.de/arbeitsgruppen/mineralogie-geodynamik/forschungsbereich/geologie-geodynamik/workgroup.html

import glob
#import re
import datetime
import os.path
#import math
#import sys
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

# Set output png image to high quality
import matplotlib
matplotlib.use("agg")

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Determine the name of the station
def name_from_port(data_folder):
    if "2100" in data_folder:
        return "Nahuelbuta"
    elif "2101" in data_folder:
        return "Santa_Gracia"
    elif "2102" in data_folder:
        return "Pan_de_Azucar"
    elif "2103" in data_folder:
        return "La_Campana"
    elif "2104" in data_folder:
        return "Wanne_Tuebingen"

class PlotOptions:
    def __init__(self):
        self.queries = ["", "", "", ""]
        self.y_labels = ["", "", "", ""]
        self.ymin = [0, 0, 0, 0]
        self.ymax = [0, 0, 0, 0]

def data_from_csv(plot_options, query):
    x_values = []
    y_values = []

    return (x_values, y_values)

def check_one_data_set(data_type, time_frame):
    pass

def check_for_missing_data(plot_options):
    check_one_data_set("battery_data", datetime.timedelta(days=1))
    check_one_data_set("multiple_data", datetime.timedelta(hours=1))

def plot_data(plot_options):
    print("Creating plots for {} in {}".format(plot_options.station_name, plot_options.folder))

    plot_options.plot_file_name = "plot1"

    plot_options.queries[0] = ["wind_speed", "multiple_data"]
    plot_options.y_labels[0] = "Wind Speed (180 min. Average), 3 m [$m/s$]"
    plot_options.ymin[0] = -1.0
    plot_options.ymax[0] = 25.0

    plot_options.queries[1] = ["wind_max", "multiple_data"]
    plot_options.y_labels[1] = "Wind Max, 3 m [$m/s$]"
    plot_options.ymin[1] = -1.0
    plot_options.ymax[1] = 25.0

    plot_options.queries[2] = ["wind_direction", "multiple_data"]
    plot_options.y_labels[2] = "Wind Direction, 3 m [degrees]"
    plot_options.ymin[2] = -10.0
    plot_options.ymax[2] = 360.0

    plot_options.queries[3] = ["battery_voltage", "battery_data"]
    plot_options.y_labels[3] = "Battery Voltage [V]"
    plot_options.ymin[3] = -1.0
    plot_options.ymax[3] = 14.0

    plot_data_full(plot_options)
    plot_data_weekly(plot_options)


    plot_options.plot_file_name = "plot2"

    plot_options.queries[0] = ["air_temperature", "multiple_data"]
    plot_options.y_labels[0] = "Air Temperature, 2 m [deg C]"
    plot_options.ymin[0] = -10.0
    plot_options.ymax[0] = None

    plot_options.queries[1] = ["air_relative_humidity", "multiple_data"]
    plot_options.y_labels[1] = "Air Rel. Humidity, 2 m [%]"
    plot_options.ymin[1] = -10.0
    plot_options.ymax[1] = 110.0

    plot_options.queries[2] = ["air_pressure", "multiple_data"]
    plot_options.y_labels[2] = "Air Pressure [mbar]"
    plot_options.ymin[2] = None
    plot_options.ymax[2] = None

    plot_options.queries[3] = ["solar_radiation", "multiple_data"]
    plot_options.y_labels[3] = "Solar Radiation [$W/m^2$]"
    plot_options.ymin[3] = -100.0
    plot_options.ymax[3] = None

    plot_data_full(plot_options)
    plot_data_weekly(plot_options)


    plot_options.plot_file_name = "plot3"

    plot_options.queries[0] = ["soil_water_content", "multiple_data"]
    plot_options.y_labels[0] = "Soil Water, 25 cm depth [$m^3/m^3$]"
    plot_options.ymin[0] = -0.005
    plot_options.ymax[0] = 0.6

    plot_options.queries[1] = ["soil_temperature", "multiple_data"]
    plot_options.y_labels[1] = "Soil Temperature, 25 cm depth [deg C]"
    plot_options.ymin[1] = -5.0
    plot_options.ymax[1] = 50.0

    plot_options.queries[2] = ["precipitation", "multiple_data"]
    plot_options.y_labels[2] = "Precipitation [mm]"
    plot_options.ymin[2] = -1.0
    plot_options.ymax[2] = None

    plot_options.queries[3] = ["li_battery_voltage", "battery_data"]
    plot_options.y_labels[3] = "Li Battery Voltage [V]"
    plot_options.ymin[3] = -1.0
    plot_options.ymax[3] = 4.0

    plot_data_full(plot_options)
    plot_data_weekly(plot_options)


def plot_data_full(plot_options):
    plot_options.file_path = "{}/{}_{}_full.png".format(plot_options.folder,
        plot_options.station_name, plot_options.plot_file_name)
    plot_options.title = "{} - Full Time Series - (as of {})".format(plot_options.station_name,
        plot_options.todays_date)
    plot_options.weekly = False
    plot_data_4_plots(plot_options)

def plot_data_weekly(plot_options):
    plot_options.file_path = "{}/{}_{}_weekly.png".format(plot_options.folder,
        plot_options.station_name, plot_options.plot_file_name)
    plot_options.title = "{} - Last Week Time Series - (as of {})".format(plot_options.station_name,
        plot_options.todays_date)
    plot_options.weekly = True
    plot_data_4_plots(plot_options)

def plot_data_4_plots(plot_options):
    # Create 4 subplots
    fig, all_axes = plt.subplots(4, 1, figsize=(10,20), sharex=True)
    plt.xticks(rotation=90)
    all_axes[0].set_title(plot_options.title)

    x_values = []
    y_values = []
    for query in plot_options.queries:
        (res1, res2) = data_from_csv(plot_options, query)
        x_values.append(res1)
        y_values.append(res2)

    # Process each subplot
    for i, ax in enumerate(all_axes):
        # print("len x_values: {}, len x_values[{}]: {}".format(len(x_values), i, len(x_values[i])))
        # print("len y_values: {}, len y_values[{}]: {}".format(len(y_values), i, len(y_values[i])))
        if len(y_values[i]) > 0:
            if plot_options.weekly:
                ax.plot(x_values[i], y_values[i], "-o")
            else:
                ax.plot(x_values[i], y_values[i])
            ax.set_ylabel(plot_options.y_labels[i])
            if not plot_options.ymin[i]:
                values = [value for value in y_values[i] if value]
                min_value = min(values) if len(values) > 0 else 0.0
                plot_options.ymin[i] = min_value * 0.9 if min_value > 0.0 else min_value * 1.1
            if not plot_options.ymax[i]:
                values = [value for value in y_values[i] if value]
                max_value = max(values) if len(values) > 0 else 0.0
                plot_options.ymax[i] = max_value * 1.1 if max_value > 0.0 else max_value * 0.9
            ax.set_ylim(plot_options.ymin[i], plot_options.ymax[i])
            ax.grid(True)

    # Set x labels for last subplot
    if plot_options.weekly:
        all_axes[3].xaxis.set_major_locator(plot_options.day_locator_daily)
    else:
        all_axes[3].xaxis.set_major_locator(plot_options.day_locator)

    all_axes[3].xaxis.set_major_formatter(plot_options.date_formatter)

    # Prepare file name and save as png
    fig.savefig(plot_options.file_path, bbox_inches='tight')
    plt.close(fig)

def send_via_email(gfx_files, message):
    send_from = "willi.kappler@uni-tuebingen.de"
    send_to = ["willi.kappler@uni-tuebingen.de"]

    msg = MIMEMultipart()
    msg["From"] = send_from
    msg["To"] = COMMASPACE.join(send_to)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = message

    msg.attach(MIMEText(message))

    for path in gfx_files:
        part = MIMEBase("application", "octet-stream")
        with open(path, "rb") as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition",
                        'attachment; filename="{}"'.format(os.path.basename(path)))
        msg.attach(part)

    smtp = smtplib.SMTP("smtpserv.uni-tuebingen.de", 25)
    smtp.starttls()
    smtp.login("", "")
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
    

if __name__ == "__main__":
    print("data plotter")

    plot_options = PlotOptions()

    # Setup how date is formatted and selected
    plot_options.day_locator = mdates.DayLocator(interval=10)
    plot_options.day_locator_daily = mdates.DayLocator()
    plot_options.date_formatter = mdates.DateFormatter("%Y.%m.%d")
    todays_date_dt = datetime.datetime.now()
    plot_options.todays_date = todays_date_dt.strftime("%Y.%m.%d %H:%M:%S")

    for folder in glob.glob("21*"):
        plot_options.folder = folder
        plot_options.station_name = name_from_port(plot_options.folder)
        check_for_missing_data(plot_options)
        plot_data(plot_options)

    #send_via_email(glob.glob("21*/*_weekly.png"), "Weather Stations: Last week data")
    #send_via_email(glob.glob("21*/*_full.png"), "Weather Stations: Full time series")

