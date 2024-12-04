import os
import pandas as pd
import time
import re #Regular Expression
from copy import deepcopy
from math import log #By default log is treated as ln() in python

#Constants and Global Variables
TIME_FORMAT_COMPLETE = '%d %B,%Y %I:%M:%S %p'
TIME_FORMAT = '%I:%M:%S %p'
FOLDER = 'Input'
Zref = 50
Z = int(input("Enter the value of 'Z': "))

def Get_Data_From_Directories(Folder_Name):
    """
    Get all the Files Data from Directories and their Sub-Directories
    Categorized by Cities.
    :param Folder_Name: (string) The name of MAIN FOLDER where all the data exist
    :return: (dict) The dictionary file of City and its DATAFRAME
    """

    print("*" * 100)
    print(f"Function: Get_Data_From_Directories() Started -> {time.strftime(TIME_FORMAT)}")
    FILES_PATH = "{FOLDER_NAME}/{CITY_NAME}/Extracted Data Sets/"
    Final_Dict = {}

    try:

        Cities = [c for c in os.listdir(Folder_Name)] #Get all the cities name in the MAIN FOLDER
        for City_Name in Cities:

            Files_List = os.listdir(FILES_PATH.format(FOLDER_NAME = Folder_Name,CITY_NAME = City_Name)) #Get all the Files name (Year-wise) of each particular location
            Final_Dict[City_Name] = {}

            for file in Files_List: #Making the JSON file of our DATA
                Final_Dict[City_Name].update({
                    file: pd.read_csv(FILES_PATH.format(FOLDER_NAME = Folder_Name, CITY_NAME = City_Name) +  str(file),sep='delimeter', engine='python',header=None)
                })

        print("Files Extracted are:")
        for x in Final_Dict.keys():
            print(f'{x} : {", ".join(Final_Dict[x].keys())}')

        print(f"Function: Get_Data_From_Directories() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
        print("*" * 100)
        return Final_Dict

    except Exception as error:
        print(f"Function: Get_Data_From_Directories() Ended with ERROR: '{error}'")
        print("*" * 100)

        return None

def Extract_Longitude_Latitude_Values(Dict_Data):
    """
    Get all the Longitude and Latitude Values of each city, extracted STATICALLY from row 2
    :param Dict_Data: (dict) The Data retrieve from the Directories in Multi-dimensional Dictionary
    :return: (dict) The dictionary of key: City and value: List of [Longitude, Latitude]
    """

    print(f"Function: Extract_Longitude_Latitude_Values() Started -> {time.strftime(TIME_FORMAT)}")
    pattern = r'Latitude\s+(\d+\.\d+)\s+Longitude\s+(\d+\.\d+)'
    Long_Lati_Dict = {}
    Data = deepcopy(Dict_Data)
    try:
        if len(Data) > 0:
            for City in Data:

                Long_Lati_Expression = str(list(Data[City].values())[0].iloc[3].values) #Get 2nd row where Longitude and Latitude Value Contains
                match = re.search(pattern, Long_Lati_Expression) #Finding the Value using REGEX Expression

                if match:
                    latitude = match.group(1)
                    longitude = match.group(2)
                    Long_Lati_Dict[City] = [longitude,latitude]
                    print(f"City: {City} -> Latitude: {latitude}, Longitude: {longitude}")

                else:
                    print(f"No Longitude and Latitude Value Found for City: {City}")

            print(f"Function: Extract_Longitude_Latitude_Values() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return Long_Lati_Dict

        else:
            print("Function: Extract_Longitude_Latitude_Values() Ended -> 'Data is not extracted from Directories, Check Get_Data_From_Directories() function'")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: Extract_Longitude_Latitude_Values() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def Transform_Data_With_New_Headers(Dict_Data):
    """
    Transform the Dataframe from (87xx Rows, 1 Column) To (87xx Rows, 8 Column) and drop the first 12 STATIC Rows
    :param Dict_Data: (dict) The Data retrieve from the Directories in Multi-dimensional Dictionary
    :return: (dict) The same dictionary with new modified values for further processing
    """

    print(f"Function: Transform_Data_With_New_Headers() Started -> {time.strftime(TIME_FORMAT)}")
    Data = deepcopy(Dict_Data)
    try:
        if len(Data) > 0:
            for City in Data:
                for File_name, File in list(Data[City].items()):

                    File = File.iloc[12:] # Removing first 12 rows
                    File = File[0].str.split(',',expand=True) # Transforming the file from 1 Column to 8 Columns

                    headers_row = File.iloc[0]  # Extract the headers from first Row of new Dataframe
                    headers = headers_row.str.split(',').explode() # Converting headers to suitable format for the Dataframe

                    File.columns = headers # Assigning new headers
                    File = File.iloc[1:] # Removing Additional header row
                    File = File.reset_index(drop=True) # Reset the S.No from 12 to 0

                    Data[City].update({File_name: File}) # Apply changes to Main Dictionary

            print(f"Function: Transform_Data_With_New_Headers() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return Data

        else:
            print("Function: Transform_Data_With_New_Headers() Ended -> 'Data is not extracted from Directories, Check Get_Data_From_Directories() function'")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: Transform_Data_With_New_Headers() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def Validate_Rows(Transform_Data):
    """
    Filter the data based on the Year present in File name, if it contains 8760 rows of that year,
    it is included otherwise excluded
    :param Transform_Data: (dict) The Transformed Data from the Transform_Data_With_New_Headers() function
                           in Multi-dimensional Dictionary
    :return: (dict) The same Multi-dimensional Dictionary with 8760 Rows Data only
    """

    print(f"Function: Validate_Rows() Started -> {time.strftime(TIME_FORMAT)}")
    Data = deepcopy(Transform_Data)
    List_To_Exclude = []
    try:
        if len(Data) > 0:

            for City in Data:
                for File in Data[City]:

                    Year = File.split('(')[-1].split(")")[0].strip() # Extract the Year within the File Name
                    df = Data[City][File] # Get the 1st Dataframe of First City in the dictionary

                    Final_df = df[df["YEAR"] == Year] # Filtering based on Year
                    Final_df = Final_df[~((Final_df["MO"] == '2') & (Final_df["DY"] == '29'))] if int(Year) % 4 == 0 else Final_df #Exclude Leap year additional 29th Date
                    Null_Values_Count = Final_df.isnull().all(axis=1).sum() # Find if there are any null valu/rows in the data
                    if len(Final_df) == 8760 and Null_Values_Count == 0 :
                        Data[City].update({File:Final_df})

                    else:
                        print(f"File: '{File}' contains {len(Final_df)} rows Only, so it is EXCLUDED")
                        List_To_Exclude.append(File)

            #Excluding the Invalid Cities
            if len(List_To_Exclude) > 0:
                for City in Data:
                    for Exclude_City in List_To_Exclude:
                        if Data[City].get(Exclude_City) is not None:
                            Data[City].pop(Exclude_City)
            else:
                print("There is no Cities to Exclude, All Files contains 8760 Rows")

            print(f"Function: Validate_Rows() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return Data

        else:
            print("Function: Validate_Rows() Ended -> 'Data is not Transformed from Transform_Data_With_New_Headers() function' Kindly check")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: Validate_Rows() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def Validate_Columns(Transform_Data):
    """
    Filter the data based on this "[T2M, PS, WD50M, WS50M]" column order in File, if it is exact same, it will be included,
    otherwise excluded
    :param Transform_Data: (dict) The Transformed Data from the Validate_Rows() function
                           in Multi-dimensional Dictionary
    :return: (dict) The same Multi-dimensional Dictionary with 8760 Rows and 4 Column only
    """

    print(f"Function: Validate_Columns() Started -> {time.strftime(TIME_FORMAT)}")
    Data = deepcopy(Transform_Data)
    List_To_Exclude = []
    Columns_To_Verify = ['T2M','PS','WD50M','WS50M']
    try:
        if len(Data) > 0:

            for City in Data:
                for File in Data[City]:

                    df = Data[City][File]  # Get the 1st Dataframe of First City in the dictionary
                    df_Columns_list = list(df.columns)[4:] # Get the last 4 columns after YEAR, MO, DY, HR

                    if df_Columns_list == Columns_To_Verify:
                        Data[City].update({File: df.iloc[:,4:]})

                    else:
                        print(f"File: '{File}' Column order are not the exact same as this -> '{Columns_To_Verify}', so it is EXCLUDED")
                        List_To_Exclude.append(File)

            # Excluding the Invalid Cities
            if len(List_To_Exclude) > 0:
                for City in Data:
                    for Exclude_City in List_To_Exclude:
                        if Data[City].get(Exclude_City) is not None:
                            Data[City].pop(Exclude_City)
            else:
                print("There is no Cities to Exclude, All Files Columns order is appropriate")

            print(f"Function: Validate_Columns() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return Data

        else:
            print("Function: Validate_Columns() Ended -> 'Data is not Transformed from Validate_Rows() function' Kindly check")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: Validate_Columns() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def Pressure_Conversion(Transform_Data):
    """
    Convert the data "Pressure PS" Column Value from data "Transform_Data" into atmospheric pressure atm
    :param Transform_Data: (dict) The Transformed Data from the Validate_Columns() function
                           in Multi-dimensional Dictionary
    :return: (dict) The same Multi-dimensional Dictionary with converted pressure values
    """

    print(f"Function: Pressure_Conversion() Started -> {time.strftime(TIME_FORMAT)}")
    Data = deepcopy(Transform_Data)
    try:
        if len(Data) > 0:

            for City in Data:
                for File in Data[City]:
                    df = Data[City][File]  # Get the 1st Dataframe of First City in the dictionary
                    df_Pressure_Column = list(df.iloc[:,1]) # Get the PS Columns Values in a list
                    df['PS'] = [(float(x) * 1000)/ 101325 for x in df_Pressure_Column] # Conversion and replacing the PS values at 'ATM' unit
                    Data[City].update({File: df})

            print(f"Function: Pressure_Conversion() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return Data

        else:
            print("Function: Pressure_Conversion() Ended -> 'Data is not Transformed from Validated_RowsColumns_Data() function' Kindly check")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: Pressure_Conversion() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def Calculate_ALPHA_Value(Transform_Data):
    """
    Calculate the ALPHA value using Zref, Uref and some constants and make it in the dictionary
    :param Transform_Data: (dict) The Transformed Data from the Pressure_Conversion() function
                           in Multi-dimensional Dictionary
    :return: (dict) A Dictionary with File name and its ALPHA Value
    """

    print(f"Function: Calculate_ALPHA_Value() Started -> {time.strftime(TIME_FORMAT)}")
    Alphas_Dict = {}

    try:
        if len(Transform_Data) > 0:

            for City in Transform_Data:
                for File in Transform_Data[City]:

                    df = Transform_Data[City][File]  # Get the 1st Dataframe of First City in the dictionary
                    df['WS50M'] = df['WS50M'].astype(float) # Convert the WS50M Column from str to float
                    Uref = df.iloc[:, -1].mean()  # Get the Average (Uref) Value of WS50M Column
                    ALPHA = (0.37 - (0.088*log(Uref)))/(1 - (0.088*log(Zref/10))) # Formula to Calculate the ALPHA Value for each file

                    # print(f"The ALPHA Value for the file: '{File}' is -> '{ALPHA}'") #Uncomment this if you want to show Alpha Value logs for each file
                    Alphas_Dict.update({File:ALPHA})

            print(f"Function: Calculate_ALPHA_Value() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return Alphas_Dict

        else:
            print("Function: Calculate_ALPHA_Value() Ended -> 'Data is not Transformed from Pressure_Conversion() function' Kindly check")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: Calculate_ALPHA_Value() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def WindSpeed50M_Conversion(Transform_Data, Alpha_Values):
    """
    Convert the data "Wind Speed at 50 WS50M" Column Value from data "Transform_Data" into new Wind speed U(z)
    :param Transform_Data: (dict) The Transformed Data from the Pressure_Conversion() function
                           in Multi-dimensional Dictionary
    :param Alpha_Values: (dict) The Alpha Values with respect to each File name
    :return: (dict) The same Multi-dimensional Dictionary with converted Wind speed values
    """

    print(f"Function: WindSpeed50M_Conversion() Started -> {time.strftime(TIME_FORMAT)}")
    Data = deepcopy(Transform_Data)

    try:
        if len(Data) > 0:

            for City in Data:
                for File in Data[City]:
                    df = Data[City][File]  # Get the 1st Dataframe of First City in the dictionary
                    Alpha = Alpha_Values[File] # Get the Alpha value with respect to File name
                    All_Uzr_Values = list(df.iloc[:,-1]) # Get all the Uzr values in a list
                    df['WS50M'] = [((Z/Zref)**Alpha) * Uzr for Uzr in All_Uzr_Values] # Conversion and replacing the WS50M values to new ones
                    Data[City].update({File: df})

            print(f"Function: WindSpeed50M_Conversion() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return Data

        else:
            print("Function: WindSpeed50M_Conversion() Ended -> 'Data is not Transformed from Pressure_Conversion() function' Kindly check")
            print("*" * 100)
            return None

    except Exception as error:
        print(f"Function: WindSpeed50M_Conversion() Ended with ERROR: '{error}'")
        print("*" * 100)
        return None

def Export_CSV_SRW_LOGS_Files(Final_Data, Alpha_Values, LongLati_Values):
    """
    The function Export_CSV_SRW_LOGS_Files() exports the CSV, SRW, and TXT logs file of Each City Data
    :param Final_Data: (dict) A finalized Multi-dimensional Dictionary of our Data
    :param Alpha_Values: {dict} A Dictionary of Alpha files with respect to file name
    :param LongLati_Values: (dict) A Dictionary of Longitude and Latitude values of each city
    :return: None
    """

    print(f"Function: Export_CSV_SRW_LOGS_Files() Started -> {time.strftime(TIME_FORMAT)}")
    FINAL_FILE_PATH = "{FOLDER_NAME}/{CITY_NAME}/Prepared Data Sets/"
    SRW_HEADER = "loc_id,city??,{CITY},Pakistan,year??,lat??,lon??,{LATITUDE},{LONGITUDE},8760\nFinalYearProject\nTemperature,Pressure,Direction,Speed\nC,atm,degrees,m/s\n2,0,{Z},{Z}\n"

    try:
        if len(Final_Data) > 0:
            for City in Final_Data:
                Final_path = FINAL_FILE_PATH.format(FOLDER_NAME=FOLDER, CITY_NAME=City)  # Make the path for Prepared Dataset
                os.makedirs(Final_path + "CSV/", exist_ok=True)  # Create the Prepared Data set directory, if not exist
                os.makedirs(Final_path + "SRW/", exist_ok=True)  # Create the Prepared Data set directory, if not exist
                time.sleep(0.25)

                #Removing the Logs file if it is already created from Previous Execution (To Avoid Data Logs Duplication)
                try:
                    os.remove(Final_path + City +" Logs.txt")
                    print(f"The Logs of File: '{City + ' Logs.txt'}' Already exist, removing old ones...")
                except FileNotFoundError:
                    print(f"The Logs of File: '{City + ' Logs.txt'}' does not exist yet. CREATING NOW...")

                for File in Final_Data[City]:

                    #Exporting CSV Files
                    print()
                    print(f"Exporting {File} File...")
                    df = Final_Data[City][File]  # Get the 1st Dataframe of First City in the dictionary
                    df.to_csv(Final_path + f"CSV/{File}", header = False, index = False) # Export All the Finalized CSV Files

                    #Exporting SRW Files
                    print(f"Exporting {File.replace('.csv','.srw')} File...")
                    with open(Final_path + f"SRW/{File.replace('.csv','.srw')}",'w') as SRW_File:
                        Data = open(Final_path + f"CSV/{File}", 'r')
                        SRW_File.write(SRW_HEADER.format(CITY = City, LATITUDE = LongLati_Values[City][1], LONGITUDE = LongLati_Values[City][0], Z = Z))
                        SRW_File.write(Data.read())

                    #Exporting TXT Logs Files
                    with open(Final_path + City + ' Logs.txt','a') as File_log:
                        File_log.write(f"Year: {File.split('(')[1].split(')')[0]}\n")
                        File_log.write(f"Max Temp: {df['T2M'].astype(float).max()}\n")
                        File_log.write(f"Min Temp: {df['T2M'].astype(float).min()}\n")
                        File_log.write(f"Average Temp: {df['T2M'].astype(float).mean()}\n")
                        File_log.write(f"Max Wind Speed: {df['WS50M'].max()}\n")
                        File_log.write(f"Min Wind Speed: {df['WS50M'].min()}\n")
                        File_log.write(f"Average Wind Speed: {df['WS50M'].mean()}\n")
                        File_log.write(f"Max Pressure: {df['PS'].max()}\n")
                        File_log.write(f"Min Pressure: {df['PS'].min()}\n")
                        File_log.write(f"Average Pressure: {df['PS'].mean()}\n\n")
                        # File_log.write(f"Alpha Value: {Alpha_Values[File]}\n") # Uncomment this, If you also wants to show Alpha Values in the Logs of each file
                        # File_log.write(f"Longitude & Latitude Values: {LongLati_Values[City]}\n\n") # Uncomment this, If you also wants to show Longitude and Latitude Values in the Logs of each file

            print(f"Function: Export_CSV_SRW_LOGS_Files() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)

        else:
            print("Function: Export_CSV_SRW_LOGS_Files() Ended -> 'Data is not Transformed from WindSpeed50M_Conversion() function' Kindly check")
            print("*" * 100)

    except Exception as error:
        print(f"Function: Export_CSV_SRW_LOGS_Files() Ended with ERROR: '{error}'")
        print("*" * 100)

def Main():

    print(f"EXECUTION STARTED AT: {time.strftime(TIME_FORMAT_COMPLETE)}")
    Raw_Data = Get_Data_From_Directories(Folder_Name= FOLDER)

    LongLati_Dict = Extract_Longitude_Latitude_Values(Dict_Data= Raw_Data)
    Transformed_Data = Transform_Data_With_New_Headers(Dict_Data = Raw_Data)
    Validated_Rows_Data = Validate_Rows(Transform_Data = Transformed_Data)
    Validated_RowsColumns_Data = Validate_Columns(Transform_Data = Validated_Rows_Data)
    Pressure_Converted_Data = Pressure_Conversion(Transform_Data = Validated_RowsColumns_Data)
    ALPHA_Values_Dict = Calculate_ALPHA_Value(Transform_Data = Pressure_Converted_Data)
    Finalized_Data = WindSpeed50M_Conversion(Transform_Data = Pressure_Converted_Data, Alpha_Values = ALPHA_Values_Dict)

    Export_CSV_SRW_LOGS_Files(Final_Data = Finalized_Data, Alpha_Values = ALPHA_Values_Dict, LongLati_Values = LongLati_Dict)
    print(f"EXECUTION ENDED AT: {time.strftime(TIME_FORMAT_COMPLETE)}")

if __name__ == '__main__':
    Main()