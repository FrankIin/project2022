from os.path import dirname, join as pjoin
from os.path import isfile, join
from datetime import datetime
from tabulate import tabulate
from nptdms import TdmsFile
import pytz
import time
import sys
import os
import scipy.io as sio
from os import listdir
import numpy as np

# gives the correct timezone based on time of computer
set(pytz.all_timezones_set)

# location of the tdms folder you want to read
folder_read = "C:/Users/researcher/Desktop/KCNR_Students_Test_Data/MONSTER_Test_Data/Sensor_Data_Test"
# location of the matlab folder where you want to save
folder_write = "C:/Users/researcher/Desktop/KCNR_Students_Test_Data/MONSTER_Test_Data/matlab"
# format for saving the matlab files in correct folder: matlab/station_name/year/month/file
folder_write_format = "%Y/%B"  # --> 2022/January
folders = folder_write_format.split("/")

# format for saving files, minutes and seconds has to be like: '%M%S' (line 347)
format_file_name = "%Y%m%d_%H%M%S"
# formatfrom is format for the date and time that is being read from tdms file (recordStartTime and wf_start_time)
format_from = "%Y-%m-%dT%H:%M:%S.000000"
# formatto is format for the date and time that is being written in matlab file (statStartTime)
format_to = "%d-%m-%Y %H:%M:%S"

# tdms keywords used for collecting data
words_tdms = ["recordStartTime", "unit_string", "wf_samples", "wf_start_time"]
# matlab keywords used for writing and collecting data
words_matlab = ["statUnits", "statSampleRate", "statChannelName", "statDataCh",
                "statStation", "statLatitude", "statLongitude", "statStartTime"]

# the amount of seconds in file after conversion , 3600 = one hour of data for each converted file
# this value has to be in sync with the files you want to read
time_in_package_sec = 3600

# fixed values
# to make sure these values are variable, these values can be changed depending on the group name with a corresponding init file for the values
station_name = "Fraeylemaborg"
value_lat = 53.215011
value_long = 6.810303


def check_for_mail(percentage, date_time, array_list):
    """
    check_for_mail checks if mail needs to be send (after merging files from same hour check if percentage is below 90 percent)

    :param percentage: average of valid percentage of channels
    :param date_time: date and time of tdms file
    :param array_list: array to save previous file and percentage
    :return: returns array_list with new values if appended
    """
    if array_list.size == 0:
        return np.append(array_list, [date_time, percentage])
        # more information about last file can be captured here ^
    else:
        # checks if current file is different from last file (to make sure last hour file has been merged if needed)
        if date_time.replace(minute=0, second=0) == array_list[0].replace(minute=0, second=0):
            array_list[0] = date_time
            array_list[1] = percentage
            return array_list
        else:
            # if the percentages of last hour file is below 90 send a mail with corresponding information
            if array_list[1] < 90:
                time_full_hour = array_list[0].replace(minute=0, second=0)
                print('File: {0} has too much corrupted data. Sending mail...'.format(
                    time_full_hour.strftime(format_to)))
            array_list = np.empty(0)
            return array_list


def get_info(file, group, key_words, full_size_time):
    """
    get_info retrieves all information needed to convert tdms file to matlab

    :param file: tdms file properties
    :param group: content of all channels in group
    :param key_words: tdms keywords used to retrieve information from tdms file
    :param full_size_time: amount of seconds in a full size matlab file (3600 sec = 1 hour of data)
    :return: returns the information that is retrieved from tdms file
    """
    # retrieve all information from file
    group_channels = group.channels()

    # timezone of the date and time being read from tdms
    timezone = pytz.timezone("UTC")
    datetime_utc = timezone.localize(datetime.strptime(
        str(file.properties[key_words[0]]), format_from))
    # key_words[0]: recordStartTime

    # change utc to correct timezone
    date_time_cf = datetime_utc.astimezone()

    # retrieve unit value from every channels                key_words[1]: unit_string
    unit_values = [file[group.name][channel.name].properties[key_words[1]]
                   for channel in group_channels]
    # retrieve sample rate from every channel
    sample_rate_values = [  # key_words[2]: wf_samples
        file[group.name][channel.name].properties[key_words[2]] for channel in group_channels]

    max_values = [full_size_time *
                  i for i in sample_rate_values]
    # retrieve start date and time from every channel                                key_words[3]: wf_start_time
    start_time_values = [timezone.localize(file[group.name][channel.name].properties[key_words[3]].astype(
        datetime)).astimezone() for channel in group_channels]

    return date_time_cf, unit_values, sample_rate_values, max_values, start_time_values


def check_previous_files(file_to_check):
    """
    check_previous_files checks is there's already a matlab file from same hour

    :param file_to_check: directory of the file that is being checked for duplicate files in same hour
    :return: returns the content of the matlab file from the same hour if file has been found
             returns False if no file from same hour has been found
    """
    try:
        # open contents if there's a file from full hour
        mat_contents = sio.loadmat(file_to_check)
        print(
            'Existing file from same hour is found! Trying to merge...')
        # return contents of the file
        return mat_contents
    except:
        # no file has been found which means that there are no other files from same hour
        return False


def append_files(previous_file, current_file, units, sample_rates, new_start_time):
    """
    append_files merges channel data from both files if headers match

    :param previous_file: contents of the previous file from same hour
    :param current_file: contents of the channels from current file
    :param units: unit values from current file
    :param sample_rates: sample rates from current file
    :param new_start_time: start times of the channels from current file
    :return: returns content of combined channel data if headers match
    """
    # retrieve header information from previous_file
    previous_unit_file = np.array(
        previous_file[words_matlab[0]])  # statUnits
    previous_sample_rates = np.array(
        [i[0] for i in previous_file[words_matlab[1]]])  # statSampleRate
    previous_channel_names = np.array([])
    for idx in range(len(previous_unit_file)):
        channel_name = previous_file[words_matlab[2] +  # statChannelName
                                     '{0}'.format(idx + 1)][0]
        previous_channel_names = np.append(
            previous_channel_names, [channel_name])

    current_channel_names = np.array(
        [channel.name for channel in current_file])

    check_units = np.array_equal(previous_unit_file, np.array(units))
    check_names = np.array_equal(previous_channel_names, current_channel_names)
    check_sample_rates = np.array_equal(
        previous_sample_rates, np.array(sample_rates))
    # check if headers are in the same in both files
    if check_units and check_names and check_sample_rates:
        combined_values = [[] for i in previous_unit_file]
        for idx in range(len(combined_values)):
            # retrieve where the first value starts from current file in a full size file
            current_file_time = new_start_time[idx].minute * \
                                60 + new_start_time[idx].second
            values_to_remove = (time_in_package_sec -
                                current_file_time) * sample_rate_values[idx]
            # retrieve all neccesary data from previous file (only retrieve data before current file)
            combined_values[idx] = [a[0] for a in previous_file[words_matlab[3] + '{0}'.format(  # statDataCh
                idx + 1)]][:-values_to_remove]
            # add previous file and current file data
            combined_values[idx] = np.append(
                combined_values[idx], current_file[idx])
        return combined_values
    else:
        print('Duplicate files are incorrect and thus cannot merge!\nNew file will be made')
        return False


# returns full size channel data values
def check_for_nan(values, start_time, sample_rate, total_time):
    """
    check_for_nan will fill up the array with nans if size is not correct

    :param values: channel values that will be checked for size
    :param start_time: the time when channel started recording
    :param sample_rate: the sample rate of the channel
    :param total_time: amount of time a file should be
    :return: returns the correct amount of channel data
    """
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
                print("File size bigger than expected")
            return new_values


def write_to_matlab(folder_write, channels, time, values, sample_rate, units, station, latitude, longitude, format,
                    folders):
    """
    write_to_matlab writes the data to specified location=
    :param folder: location in which files will be saved
    :param channels: channel data of group
    :param group_name: group name
    :param time: time of recording
    :param values: correct channel data
    :param sample_rate: sample rate of channels
    :param units: unit values of channels
    :param station: station name of datalogger
    :param latitude: latitude of station
    :param longitude: longitude of station
    :param format: correct format for saving matlab files
    """
    file = time.strftime(format)
    matlab_dir = pjoin(dirname(sio.__file__), folder_write, station)

    dict_values = {
        words_matlab[1]: sample_rate,  # statSampleRate
        words_matlab[0]: units,  # statUnits
        words_matlab[4]: station,  # statStation
        words_matlab[5]: latitude,  # statLatitude
        words_matlab[6]: longitude,  # statLongitude
    }
    for idx, channel in enumerate(channels, start=1):
        dict_values[words_matlab[2] + '{0}'.format(
            idx)] = channel.name  # statChannelname
        dict_values[words_matlab[7] + '{0}'.format(  # statStartTime
            idx)] = start_time_values[idx - 1].strftime(format_to)
        dict_values[words_matlab[3] +
                    '{0}'.format(idx)] = values[idx - 1]  # statDataCh

    try:
        os.mkdir(matlab_dir)
        for folder in folders:
            os.mkdir(os.path.join(matlab_dir, time.strftime(folder)))
            matlab_dir = pjoin(matlab_dir, time.strftime(folder))
        matlab_file = pjoin(matlab_dir, file)
        sio.matlab.savemat(matlab_file + '.mat', dict_values, oned_as='column')
    except FileExistsError:
        matlab_file = pjoin(matlab_dir, '/'.join(time.strftime(folder)
                                                 for folder in folders), file)
        sio.matlab.savemat(matlab_file + '.mat', dict_values, oned_as='column')


def write_to_terminal(values, channel_names, group_name, date_time):
    """
    write_to_terminal writes useful information in the terminal about the channel values

    :param values: channel data of group
    :param channel_names: channel names of group
    :return: returns valid values in percentage
    """
    invalid_values = [np.count_nonzero(np.isnan(v)) for v in values]
    valid_values = [+len(v) - invalid_values[idx]
                    for idx, v in enumerate(values)]
    percentage_valid = [v * 100 / (invalid_values[idx] + v)
                        for idx, v in enumerate(valid_values)]

    # define header names
    col_names = ["Channels", "Valid Values",
                 "Invalid Values", "Valid Values [in %]"]

    zipped_list = zip(channel_names, valid_values,
                      invalid_values, percentage_valid)

    # prints all properties of tdms file
    # recordStartTime is in UTC, 2 hours earlier compared to NL
    # print(tabulate(tdms_file.properties.items()))

    # display table
    print("Datalogger:\t", group_name)
    print('{0} {1}'.format(date_time.strftime(
        '%a %d %b %Y, %H:%M:%S'), time.tzname))
    print(tabulate(zipped_list, headers=col_names))
    print("\n\n")
    return percentage_valid


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

    dupe_check = np.empty(0)
    for file in tdms_files:
        # open tdms file (Does not read the whole file immediately but has the file open to read from when asked)
        # preferable for large tdms files which cannot save all values in memory
        with TdmsFile.open('{0}/{1}'.format(folder_read, file)) as tdms_file:

            # make a file for each group
            for group in tdms_file.groups():
                # checks if mail for percentages has to be send
                try:
                    dupe_check = check_for_mail(np.average(
                        percentage_valid), date_time_cf, dupe_check)
                except NameError:
                    print('Skipping first run')

                date_time_cf, unit_values, sample_rate_values, max_values, start_time_values = get_info(
                    tdms_file, group, words_tdms, time_in_package_sec)

                # check if there's more than one file from same hour
                previous_file_from_hour = check_previous_files(
                    pjoin(folder_write, station_name, date_time_cf.strftime(folder_write_format),
                          date_time_cf.strftime("%Y%m%d_%H0000")))

                if previous_file_from_hour:
                    # retrieves all the information of previous file from same hour
                    # valid_check is true when headers of previous file matches current file
                    new_channel_values = append_files(
                        previous_file_from_hour, group.channels(), unit_values, sample_rate_values, start_time_values)
                    valid_check = True if new_channel_values else False
                else:
                    # when no other files are found from same hour
                    new_channel_values = False
                    valid_check = True

                if valid_check:
                    # change format for title of matlab file -> minutes and seconds are reset
                    new_format = format_file_name.replace("%M%S", "0000")
                else:
                    # valid_check is only false when two files from same hour have different headers title of matlab
                    # file will maintain the minutes and seconds to prevent overriding files that cannot merge
                    new_format = format_file_name

                # file needs to be checked for full contents
                # if not, nans will be added to fill up file
                correctedValues = [[] for x in group.channels()]
                for idx, channel in enumerate(group.channels()):
                    # when more files in same hour are found, pass through the correct data
                    if new_channel_values and valid_check:
                        channel = new_channel_values[idx]

                        # change time from current file to previous file to fill up correctly with nans
                        start_time_values[idx] = start_time_values[idx].replace(
                            minute=0, second=0)

                    # returns data filled up correctly with nans for full file size
                    channel_values = check_for_nan(
                        channel, start_time_values[idx], sample_rate_values[idx], time_in_package_sec)
                    correctedValues[idx] = np.append(
                        correctedValues[idx], channel_values)

                # writes all channel values to matlab file
                write_to_matlab(folder_write, group.channels(), date_time_cf, correctedValues, sample_rate_values,
                                unit_values, station_name, value_lat, value_long, new_format, folders)

                # writes useful information to terminal
                percentage_valid = write_to_terminal(correctedValues, [
                    channel.name for channel in group.channels()], group.name, date_time_cf)
