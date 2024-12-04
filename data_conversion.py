import os
import pandas as pd
from copy import deepcopy
import time

#Constants and Global Variables
TIME_FORMAT_COMPLETE = '%d %B,%Y %I:%M:%S %p'
TIME_FORMAT = '%I:%M:%S %p'
FOLDER = 'FinalYearProject(FYP)Data'

def Get_Prepared_Data_From_Directories(Folder_Name):
    """
    Get all the Files Data from Directories and their Sub-Directories
    Categorized by Cities.
    :param Folder_Name: (string) The name of MAIN FOLDER where all the data exist
    :return: (dict) The dictionary file of City and its DATAFRAME
    """

    print("*" * 100)
    print(f"Function: Get_Prepared_Data_From_Directories() Started -> {time.strftime(TIME_FORMAT)}")
    FILES_PATH = "{FOLDER_NAME}/{CITY_NAME}/Prepared Data Sets/CSV/"
    Final_Dict = {}

    try:

        Cities = [c for c in os.listdir(Folder_Name)] #Get all the cities name in the MAIN FOLDER
        for City_Name in Cities:

            Files_List = os.listdir(FILES_PATH.format(FOLDER_NAME = Folder_Name,CITY_NAME = City_Name)) #Get all the Files name (Year-wise) from CSV folder of Prepared Dataset of each particular location
            Final_Dict[City_Name] = {}

            for file in Files_List: #Making the JSON file of our DATA
                Final_Dict[City_Name].update({
                    file: pd.read_csv(FILES_PATH.format(FOLDER_NAME = Folder_Name, CITY_NAME = City_Name) +  str(file),sep=',', engine='python',header=None)
                })

        print("Files Extracted are:")
        for x in Final_Dict.keys():
            print(f'{x} : {", ".join(Final_Dict[x].keys())}')

        print(f"Function: Get_Prepared_Data_From_Directories() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
        print("*" * 100)
        return Final_Dict

    except Exception as error:
        print(f"Function: Get_Prepared_Data_From_Directories() Ended with ERROR: '{error}'")
        print("*" * 100)

        return None

def Extract_Temperature_Average_Values(Dict_Data):
    """
    Get all the Temperature Average Values of each city, extracted STATICALLY from column 1
    :param Dict_Data: (dict) The Data retrieve from the Directories in Multi-dimensional Dictionary
    :return: (dict) The dictionary of key: City and value: List of Temperature Averages with respect to month
    """

    print(f"Function: Extract_Temperature_Average_Values() Started -> {time.strftime(TIME_FORMAT)}")
    CitiesTemperature_Dict = {}
    Data = deepcopy(Dict_Data)
    try:
        if len(Data) > 0:
            for City in Data:
                CitiesTemperature_Dict.update({City: [[year.split('(')[1].split(')')[0],
                                                       file.iloc[0:744, 0].mean(), file.iloc[744:1416, 0].mean(),
                                                       file.iloc[1416:2160, 0].mean(), file.iloc[2160:2880, 0].mean(),
                                                       file.iloc[2880:3624, 0].mean(), file.iloc[3624:4344, 0].mean(),
                                                       file.iloc[4344:5088, 0].mean(), file.iloc[5088:5832, 0].mean(),
                                                       file.iloc[5832:6552, 0].mean(), file.iloc[6552:7296, 0].mean(),
                                                       file.iloc[7296:8016, 0].mean(), file.iloc[8016:8760, 0].mean()]
                                                      for year, file in list(Data[City].items())]})
                
                print(f"City: {City} Temperature Averages Extracted")

            print(f"Function: Extract_Temperature_Average_Values() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return CitiesTemperature_Dict

        else:
            print("Function: Extract_Temperature_Average_Values() Ended -> 'Data is not extracted from Directories, Check Get_Prepared_Data_From_Directories() function'")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: Extract_Temperature_Average_Values() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def Extract_Wind_Average_Values(Dict_Data):
    """
    Get all the Wind Speed Averages Values of each city, extracted STATICALLY from last column
    :param Dict_Data: (dict) The Data retrieve from the Directories in Multi-dimensional Dictionary
    :return: (dict) The dictionary of key: City and value: List of Wind Speed Averages with respect to month
    """

    print(f"Function: Extract_Wind_Average_Values() Started -> {time.strftime(TIME_FORMAT)}")
    CitiesWindSpeed_Dict = {}
    Data = deepcopy(Dict_Data)
    try:
        if len(Data) > 0:
            for City in Data:
                CitiesWindSpeed_Dict.update({City: [[year.split('(')[1].split(')')[0],
                                                       file.iloc[0:744, -1].mean(), file.iloc[744:1416, -1].mean(),
                                                       file.iloc[1416:2160, -1].mean(), file.iloc[2160:2880, -1].mean(),
                                                       file.iloc[2880:3624, -1].mean(), file.iloc[3624:4344, -1].mean(),
                                                       file.iloc[4344:5088, -1].mean(), file.iloc[5088:5832, -1].mean(),
                                                       file.iloc[5832:6552, -1].mean(), file.iloc[6552:7296, -1].mean(),
                                                       file.iloc[7296:8016, -1].mean(), file.iloc[8016:8760, -1].mean()]
                                                      for year, file in list(Data[City].items())]})

                print(f"City: {City} Wind Speed Averages Extracted")

            print(f"Function: Extract_Wind_Average_Values() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return CitiesWindSpeed_Dict

        else:
            print("Function: Extract_Wind_Average_Values() Ended -> 'Data is not extracted from Directories, Check Get_Prepared_Data_From_Directories() function'")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: Extract_Wind_Average_Values() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def Export_Monthly_Temperatures_CSV(Temp_Dict):
    """
    The function Export_Monthly_Temperatures_CSV() exports the CSV file of Each City Data
    :param Temp_Dict: (dict) A finalized Multi-dimensional Dictionary of our Temperature Averages
    :return: None
    """

    print(f"Function: Export_Monthly_Temperatures_CSV() Started -> {time.strftime(TIME_FORMAT)}")
    FINAL_FILE_PATH = "{FOLDER_NAME}/{CITY_NAME}/Summarized Data/"
    Header = ["YEAR", "JAN °C", "FEB °C", "MAR °C", "APR °C", "MAY °C", "JUN °C", "JUL °C", "AUG °C", "SEP °C", "OCT °C", "NOV °C", "DEC °C", "Annual °C"]
    try:
        if len(Temp_Dict) > 0:
            for City in Temp_Dict:
                Final_path = FINAL_FILE_PATH.format(FOLDER_NAME=FOLDER,CITY_NAME=City)  # Make the path for Prepared Dataset
                os.makedirs(Final_path, exist_ok=True)  # Create the Summarized Data set directory, if not exist
                time.sleep(0.25)

                df = pd.DataFrame(Temp_Dict[City])
                Annual_Avg = pd.Series([df.iloc[i, 1:].mean() for i in range(0, len(df))])
                df[''] = Annual_Avg
                df.to_csv(Final_path + f"Monthly_Temperature_{City}.csv", header=Header,index=False)  # Export All the Finalized CSV Files
                print(f"Monthly_Temperature_{City}.csv Created Successfully")

            print(f"Function: Export_Monthly_Temperatures_CSV() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)

        else:
            print("Function: Export_Monthly_Temperatures_CSV() Ended -> 'Data is not Transformed from Extract_Temperature_Average_Values() function' Kindly check")
            print("*" * 100)

    except Exception as error:
        print(f"Function: Export_Monthly_Temperatures_CSV() Ended with ERROR: '{error}'")
        print("*" * 100)

def Export_Monthly_WindSpeed_CSV(WindSpeed_Dict):
    """
    The function Export_Monthly_WindSpeed_CSV() exports the CSV file of Each City Data
    :param WindSpeed_Dict: (dict) A finalized Multi-dimensional Dictionary of our Wind Speed Averages
    :return: None
    """

    print(f"Function: Export_Monthly_WindSpeed_CSV() Started -> {time.strftime(TIME_FORMAT)}")
    FINAL_FILE_PATH = "{FOLDER_NAME}/{CITY_NAME}/Summarized Data/"
    Header = ["YEAR", "JAN m/s", "FEB m/s", "MAR m/s", "APR m/s", "MAY m/s", "JUN m/s", "JUL m/s", "AUG m/s", "SEP m/s", "OCT m/s", "NOV m/s", "DEC m/s", "Annual m/s"]
    try:
        if len(WindSpeed_Dict) > 0:
            for City in WindSpeed_Dict:
                Final_path = FINAL_FILE_PATH.format(FOLDER_NAME=FOLDER,CITY_NAME=City)  # Make the path for Prepared Dataset
                os.makedirs(Final_path, exist_ok=True)  # Create the Summarized Data set directory, if not exist
                time.sleep(0.25)

                df = pd.DataFrame(WindSpeed_Dict[City])
                Annual_Avg = pd.Series([df.iloc[i, 1:].mean() for i in range(0, len(df))])
                df[''] = Annual_Avg
                df.to_csv(Final_path + f"Monthly_WindSpeed_{City}.csv", header=Header,index=False)  # Export All the Finalized CSV Files
                print(f"Monthly_WindSpeed_{City}.csv Created Successfully")

            print(f"Function: Export_Monthly_WindSpeed_CSV() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)

        else:
            print("Function: Export_Monthly_WindSpeed_CSV() Ended -> 'Data is not Transformed from Extract_Wind_Average_Values() function' Kindly check")
            print("*" * 100)

    except Exception as error:
        print(f"Function: Export_Monthly_WindSpeed_CSV() Ended with ERROR: '{error}'")
        print("*" * 100)

def Main():

    print(f"EXECUTION STARTED AT: {time.strftime(TIME_FORMAT_COMPLETE)}")

    Prepared_Data = Get_Prepared_Data_From_Directories(Folder_Name= FOLDER)
    Temperature_Averages_Dict = Extract_Temperature_Average_Values(Dict_Data= Prepared_Data)
    Wind_Averages_Dict = Extract_Wind_Average_Values(Dict_Data= Prepared_Data)

    Export_Monthly_Temperatures_CSV(Temp_Dict = Temperature_Averages_Dict)
    Export_Monthly_WindSpeed_CSV(WindSpeed_Dict = Wind_Averages_Dict)

    print(f"EXECUTION ENDED AT: {time.strftime(TIME_FORMAT_COMPLETE)}")

if __name__ == '__main__':
    Main()