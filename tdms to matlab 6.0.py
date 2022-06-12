from os.path import isfile, join
from os.path import dirname, join as pjoin
from datetime import datetime
from tabulate import tabulate
from nptdms import TdmsFile
import scipy.io as sio
from os import listdir
import numpy as np
import pytz
import math
import time
import sys

# gives the correct timezone based on time of computer
set(pytz.all_timezones_set)

# location of the tdms folder you want to read
folder_read = "C:/Users/Vosko/Desktop/School/measurements/testdata/Sensor_Data_Test/Sensor_Data_Test"
# location of the matlab folder where you want to save
folder_write = "C:/Users/Vosko/Desktop/School/measurements/testdata/Sensor_Data_Test/Matlab/"

# format for saving files
filename_format = "%Y%m%d_%H%M%S"

# formatfrom is format for the date and time that is being read from tdms file (recordStartTime and wf_start_time)
format_from = "%Y-%m-%dT%H:%M:%S.000000"
# formatto is format for the date and time that is being written in matlab file (statStartTime)
format_to = "%d-%m-%Y %H:%M:%S"

# tdms keywords used for collecting data
tdms_words = ["recordStartTime", "unit_string", "wf_samples", "wf_start_time"]

# matlab keywords used for writing and collecting data
matlab_words_stat = ["statUnits", "statSampleRate", "statChannelName", "statDataCh",
                     "statGroupName", "statStation", "statLatitude", "statLongitude", "statStartTime"]


# the amount of seconds in file after conversion , 3600 = one hour of data for each converted file
# this value has to be in sync with the files you want to read
time_in_package_sec = 3600

# fixed values
# to make sure these values are variable, these values can be changed depending on the group name with a corresponding init file for the values
station_name = "Fraeylemaborg"
value_lat = 53.215011
value_long = 6.810303


# checking if there's already information from full hour file
def check_previous_files(current_group_name, current_file_name, folder_write):
    # replace minutes and second from current file to zeros
    check_file = current_file_name.strftime("%Y%m%d_%H0000")
    try:
        # open contents if there's a file from full hour
        data_dir = pjoin(dirname(sio.__file__),
                         folder_write, current_group_name)
        mat_fname = pjoin(data_dir, check_file)
        mat_contents = sio.loadmat(mat_fname)
        print('{0} found!'.format(check_file))
        # return contents of the file
        return mat_contents
    except:
        # no file has been found which means that there are no other files from same hour
        return False


def get_previous_file(previous_file, current_file, units, frame_rates, new_start_time):
    # retrieve header information from previous_file
    previous_unit_file = np.array(
        previous_file[matlab_words_stat[0]])  # statUnits
    previous_sample_rates = np.array(
        [i[0] for i in previous_file[matlab_words_stat[1]]])  # statSampleRate
    previous_channel_names = np.array([])
    for idx in range(len(previous_unit_file)):
        channel_name = previous_file[matlab_words_stat[2] +   # statChannelName
                                     '{0}'.format(idx + 1)][0]  
        previous_channel_names = np.append(
            previous_channel_names, [channel_name])

    current_channel_names = np.array(
        [channel.name for channel in current_file])

    check_units = np.array_equal(previous_unit_file, np.array(units))
    check_names = np.array_equal(previous_channel_names, current_channel_names)
    check_sample_rates = np.array_equal(
        previous_sample_rates, np.array(frame_rates))
    if check_units and check_names and check_sample_rates:
        previous_values = [[] for i in previous_unit_file]
        for idx in range(len(previous_values)):
            current_file_time = new_start_time[idx].minute * \
                60 + new_start_time[idx].second
            values_to_remove = (time_in_package_sec -
                                current_file_time) * sample_rate_values[idx]
            previous_values[idx] = [a[0] for a in previous_file[matlab_words_stat[3] + '{0}'.format(  # statDataCh
                idx + 1)]][:-values_to_remove]
            previous_values[idx] = np.append(
                previous_values[idx], current_file[idx])
        return True, previous_values
    else:
        print('Duplicate files are incorrect and thus cannot merge!\nNew file will be made')
        return False, False


def check_for_nan(values, start_time, sample_rate, total_time):
    if total_time * sample_rate == len(values):
        return values
    else:
        seconds_passed = (start_time.minute * 60) + start_time.second
        values_passed = seconds_passed * sample_rate
        empty_array = np.empty(values_passed)
        empty_array[:] = np.NaN
        new_values = np.append(empty_array, values)
        if len(new_values) == total_time * sample_rate:
            return new_values
        else:
            missing_values = (total_time * sample_rate) - len(new_values)
            try:
                empty_array = np.empty(missing_values)
                empty_array[:] = np.NaN
                new_values = np.append(new_values, empty_array)
            except:
                print(sys.exc_info()[0], "occurred")
            return new_values


def write_to_terminal(values):
    invalid_values = [0 for i in values]
    valid_values = [0 for i in values]
    percentage_valid = [0 for i in values]

    for idx, v in enumerate(values):
        invalid_value = 0
        for a in range(len(v)):
            if math.isnan(v[a]):
                invalid_value += 1
        invalid_values[idx] = np.add(invalid_values[idx], invalid_value)
        valid_values[idx] = (+len(v) - invalid_values[idx])
        percentage_valid[idx] = valid_values[idx] / \
            (invalid_values[idx] + valid_values[idx]) * 100

        # define header names
    col_names = ["Channels", "Valid Values",
                 "Invalid Values", "Valid Values [in %]"]

    zipped_list = zip([channel.name for channel in group.channels()],
                      valid_values, invalid_values, percentage_valid)

    # prints all properties of tdms file
    # recordStartTime is in UTC, 2 hours earlier compared to NL
    # print(tabulate(tdms_file.properties.items()))

    # display table
    print(tabulate(zipped_list, headers=col_names))


def write_to_matlab(channels, group_name, time, values, sample_rate, units, station, latitude, longitude, format, valid):
    file = time.strftime(format)
    if valid == False:
        file += "-1"

    dict_values = {
        matlab_words_stat[4]: group_name,  # statGroupName
        matlab_words_stat[1]: sample_rate,  # statSampleRate
        matlab_words_stat[0]: units,  # statUnits
        matlab_words_stat[5]: station,  # statStation
        matlab_words_stat[6]: latitude,  # statLatitude
        matlab_words_stat[7]: longitude,  # statLongitude
    }
    for idx, channel in enumerate(channels, start=1):
        dict_values[matlab_words_stat[2] + '{0}'.format(
            idx)] = channel.name  # statChannelname
        dict_values[matlab_words_stat[8] + '{0}'.format(  # statStartTime
            idx)] = start_time_values[idx - 1].strftime(format_to)
        dict_values[matlab_words_stat[3] +
                    '{0}'.format(idx)] = values[idx - 1]  # statDataCh

    sio.matlab.savemat('{0}/{1}/{2}.mat'.format(folder_write,
                       group_name, file), dict_values, oned_as='column')


# if statement will make sure that nothing will run when importing this file
if __name__ == '__main__':

    # read all files in folder for checking multiple files in the same hour
    read_all_files = [f for f in listdir(
        folder_read) if isfile(join(folder_read, f))]

    # remove invalid files
    tdms_files = []
    for file in read_all_files:
        if "tdms" in file and "_index" not in file:
            tdms_files.append(file)

    for file in tdms_files:
        # open tdms file (Does not read the whole file immediately but has the file open to read from when asked)
        # preferrable for large tdms files which cannot save all values in memory
        with TdmsFile.open('{0}/{1}'.format(folder_read, file)) as tdms_file:

            # make a file for each group
            for group in tdms_file.groups():
                print("\n\nDatalogger:\t", group.name)

                # retrieve all information from file
                group_channels = group.channels()

                # timezone of the date and time being read from tdms
                timezone = pytz.timezone("UTC")
                datetime_utc = timezone.localize(datetime.strptime(
                    str(tdms_file.properties[tdms_words[0]]), format_from))
                # tdms_words[0]: recordStartTime

                # change utc to correct timezone
                date_time = datetime_utc.astimezone()
                print('{0} {1}'.format(date_time.strftime(
                    '%a %d %b %Y, %H:%M:%S'), time.tzname))

                # retrieve unit value from every channels                      tdms_words[1]: unit_string
                unit_values = [tdms_file[group.name][channel.name].properties[tdms_words[1]]
                               for channel in group_channels]
                # retrieve sample rate from every channel
                sample_rate_values = [                           # tdms_words[2]: wf_samples
                    tdms_file[group.name][channel.name].properties[tdms_words[2]] for channel in group_channels]
                max_values = [time_in_package_sec *
                              i for i in sample_rate_values]
                # retrieve start date and time from every channel                                     tdms_words[3]: wf_start_time
                start_time_values = [timezone.localize(tdms_file[group.name][channel.name].properties[tdms_words[3]].astype(
                    datetime)).astimezone() for channel in group_channels]

                # check if there's more than one file an hour
                previous_file_from_hour = check_previous_files(
                    group.name, date_time, folder_write)

                if previous_file_from_hour:
                    # retrieves all the information of previous file in same hour
                    # valid_check is true when previous file matches current file
                    valid_check, new_channel_values = get_previous_file(
                        previous_file_from_hour, group_channels, unit_values, sample_rate_values, start_time_values)
                else:
                    # when no other files are found from same hour
                    new_channel_values = False
                    valid_check = True

                # file needs to be checked for full contents
                # if not, nans will be added to fill up file
                correctedValues = [[] for x in group_channels]

                # change format for the title of the matlab files -> minutes and hours are reset
                new_format = filename_format.replace("%M%S", "0000")
                for idx, channel in enumerate(group_channels):
                    # when more files in same hour are found, pass through the correct data
                    if new_channel_values:
                        channel = new_channel_values[idx]

                        # change time from current file to previous file to fill up correctly with nans
                        start_time_values[idx] = start_time_values[idx].replace(
                            minute=0, second=0)

                    # returns data filled up correctly with nans for full file size
                    channel_values = check_for_nan(
                        channel, start_time_values[idx], sample_rate_values[idx], time_in_package_sec)
                    correctedValues[idx] = np.append( correctedValues[idx], channel_values)
                # writes usefull information to terminal
                write_to_terminal(correctedValues)
                
                # writes all channel values to matlab file
                write_to_matlab(group_channels, group.name, date_time, correctedValues, sample_rate_values,
                                unit_values, station_name, value_lat, value_long, new_format, valid_check)
