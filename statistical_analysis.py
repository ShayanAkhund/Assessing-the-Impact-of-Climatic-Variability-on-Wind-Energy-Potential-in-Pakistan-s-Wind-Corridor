import os
import pandas as pd
from PIL import Image
import time
import numpy as np
from copy import deepcopy
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression

TIME_FORMAT_COMPLETE = '%d %B,%Y %I:%M:%S %p'
TIME_FORMAT = '%I:%M:%S %p'
FOLDER = 'InputFiles'
FILES_PATH = "{FOLDER_NAME}/{CITY_NAME}/"
COLLAGE_PATH = "{FOLDER_NAME}/COLLAGE_IMGS/"

def import_files():
    '''
    It will find all the file present in the INPUT FOLDER and then save it with its data in a dictionary
    :return: (dict) -> 1 for Excel files and 1 for CSV Files (Total 2 dictionary)
    '''

    print(f"Function: import_files() Started -> {time.strftime(TIME_FORMAT)}")

    csv_files = {}
    xlsx_files = {}
    try:
        cities = os.listdir(FOLDER)
        if len(cities) > 0:
            for city in cities:

                Files = os.listdir(FILES_PATH.format(FOLDER_NAME = FOLDER, CITY_NAME = city))
                csv_files.update({File:pd.read_csv(FILES_PATH.format(FOLDER_NAME = FOLDER, CITY_NAME = city) + File, engine = 'python', encoding = 'latin1') for File in Files if
                             File.split('.')[-1].lower() == "csv"})

                xlsx_files.update({File:pd.read_excel(FILES_PATH.format(FOLDER_NAME = FOLDER, CITY_NAME = city) + File) for File in Files if
                             File.split('.')[-1].lower() == "xlsx"})

            print(f"Function: import_files() Total CSV Files Imported: {len(csv_files)}")
            print(f"Function: import_files() Total EXCEL Files Imported: {len(xlsx_files)}")
            print(f"Function: import_files() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
            print("*" * 100)
            return csv_files,xlsx_files

        else:
            print(f"Function: import_files() Ended as there is no City Available in the folder -> {FOLDER}")
            print("*" * 100)
            return csv_files,xlsx_files

    except Exception as Error:
        print(f"Function: import_files() Ended with an Error -> {Error}")
        print("*" * 100)
        return csv_files, xlsx_files

def extract_rcov(data_list : list):
    '''
    It will extract the RCOV data list from each file and return a dictionary
    :param data_list: (list) -> Contains 2 dictionary, one for CSV files and one for Excel files
    :return: (dict) -> It will return a dictionary of RCOV Data with its file name
    '''

    print(f"Function: extract_rcov() Started -> {time.strftime(TIME_FORMAT)}")
    data = deepcopy(data_list)
    rcov_data = {}

    try:
        os.makedirs(os.path.join(FOLDER,"RCOV_DATA"), exist_ok=True)  # Create the RCOV Data directory, if not exist
        for data_dict in data:
            if len(data_dict) > 0:
                for file_name, file in data_dict.items():

                    months = list(file.columns)[1:-1]
                    months_data = [file[month] for month in months]
                    rcov_data.update({file_name.split(".")[0]:
                                [round(float((np.median(np.abs(data - np.median(data)))
                                 / np.median(data)) * 100),3) for data in months_data]})

                    with open(os.path.join(os.path.join(FOLDER,"RCOV_DATA"),file_name.split('.')[0] + ".txt"),'w') as rcov_file:
                        print(f"Exporting RCOV File: '{file_name.split('.')[0] + '.txt'}'")
                        rcov_file.write(str(rcov_data[file_name.split(".")[0]]).replace("[",'').replace("]",""))

            else:
                print(f"Function: extract_rcov() Either CSV or XLSX file is empty -> kindly check import_files() function")

        print(f"Function: extract_rcov() Total RCOV Exported are: {len(rcov_data)} Ended Successfully -> {time.strftime(TIME_FORMAT)}")
        print("*" * 100)
        return rcov_data

    except Exception as Error:
        print(f"Function: extract_rcov() Ended with an Error -> {Error}")
        print("*" * 100)
        return rcov_data

def export_plotted_graphs(data_list : list, rcov_data : dict):

    print(f"Function: export_plotted_graphs() Started -> {time.strftime(TIME_FORMAT)}")

    try:
        for data in data_list:
            if len(data) > 0:
                for file_name, file in data.items():

                    file_values = file.iloc[:, 1:-1]
                    rcov = rcov_data[file_name.split('.')[0]]
                    Years = list(file['YEAR'])
                    Months = [x.split()[0] for x in list(file.columns[1:-1])]
                    Month_Indexes = np.array(range(0, 12))

                    fig, ax1 = plt.subplots(figsize=(12, 6)) # Create the plot
                    ax2 = ax1.twinx() # Create secondary axis for RCOV
                    bar_width = 1 / len(Months)  # Adjust the bar width

                    trend_x = np.array(range(1, 13)).reshape(-1, 1)
                    trend_y = np.mean(file_values.values, axis=0)
                    model = LinearRegression().fit(trend_x, trend_y)
                    trend = model.predict(trend_x)

                    if "production" in file_name.lower():
                        # Create a bar chart for Production
                        for i, value in enumerate(file_values.values):
                            ax1.bar(Month_Indexes + i * bar_width, value, bar_width, label=Years[i])

                        ax1.plot(Months, trend, color='black', linewidth='2.5', linestyle='--', label='Overall Trend')

                    else:
                        # Create a plot for WindSpeed, Temperature, and Capacity Factor
                        for i, data in enumerate(file_values.values):
                            ax1.plot(Months, data, label=f'{Years[i]}', marker='o', linestyle='-',
                                     linewidth=1, markersize=6)

                        # Plotting overall trend line
                        ax1.plot(Months, trend, color='black', linewidth='2.5', linestyle='--', label='Overall Trend')

                    # Plotting RCOV Line
                    ax2.plot(Months, rcov, color='green', linewidth='4', linestyle=':', label='RCoV')

                    # Add labels and title
                    ax1.set_xlabel('Month')
                    if 'speed' in file_name.lower():
                        ax1.set_ylabel('Wind Speed (m/s)')
                        plt.title('Wind Speed')
                    elif 'temperature' in file_name.lower():
                        ax1.set_ylabel('Temperature (°C)')
                        plt.title('Temperature')
                    elif 'factor' in file_name.lower():
                        ax1.set_ylabel('Capacity Factor (%)')
                        plt.title('Capacity Factor')
                    elif 'production' in file_name.lower():
                        ax1.set_ylabel('Production (GWh)')
                        plt.title('Production')

                    file_name_exact = file_name.split('.')[0].replace("_"," ")

                    #Setting Secondary Axis label
                    ax2.set_ylabel('RCoV (%)', color='g')

                    # Show legend
                    lines1, labels1 = ax1.get_legend_handles_labels()
                    lines2, labels2 = ax2.get_legend_handles_labels()
                    plt.legend(lines1 + lines2, labels1 + labels2, loc='upper right', prop={'size': 6})

                    # Add gridlines
                    ax1.grid(True)

                    print(f'Exporting Graph: "{file_name_exact + ".png"}"')
                    plt.savefig(FILES_PATH.format(FOLDER_NAME = FOLDER,CITY_NAME = file_name.split("_")[0]) + file_name_exact + ".png")
                    plt.close()
            else:
                print(f"Function: export_plotted_graphs() Either CSV or XLSX file is empty -> kindly check import_files() function")

        print(f"Function: export_plotted_graphs() Ended Successfully -> {time.strftime(TIME_FORMAT)}")
        print("*" * 100)

    except Exception as Error:
        print(f"Function: export_plotted_graphs() Ended with an Error -> {Error}")
        print("*" * 100)

def export_final_plotted_graphs_collage():
    '''
    It will find all the graph.png files present in each City Folder and and then make a collage of them and export them
    '''

    print(f"Function: export_final_plotted_graphs_collage() Started -> {time.strftime(TIME_FORMAT)}")

    try:
        os.makedirs(COLLAGE_PATH.format(FOLDER_NAME=FOLDER), exist_ok=True)
        cities = sorted(os.listdir(FOLDER))

        if "RCOV_DATA" in cities:  # Remove the RCOV_DATA folder from list of Cities for finding graph images
            cities.remove('RCOV_DATA')
        elif "COLLAGE_IMGS" in cities: # Remove the COLLAGE_IMGS folder from list of Cities for finding graph images
            cities.remove('COLLAGE_IMGS')

        if len(cities) > 0:
            for city in cities:
                if "COLLAGE_IMGS" in cities: # Remove the COLLAGE_IMGS folder from list of Cities
                    cities.remove('COLLAGE_IMGS')

                #Getting graph files name
                graphs = [graph for graph in os.listdir(FILES_PATH.format(FOLDER_NAME=FOLDER, CITY_NAME=city)) if str(graph).lower().endswith('.png')]

                if len(graphs) > 0:

                    # Define the custom sort order
                    sort_order = {
                        "Temperature": 0,
                        "WindSpeed": 1,
                        "Production": 2,
                        "Capacity Factor": 3
                    }

                    # Sort the list using the custom order
                    graphs = sorted(graphs, key=lambda x: sort_order[next((key for key in sort_order if key in x), -1)])

                    #Getting the list of 4 images for each city
                    images = [Image.open(os.path.join(os.path.join(FOLDER, city), graph)) for graph in graphs]
                    width, height = images[0].size

                    #Defining the Collage image width, height and object
                    collage_width = width * 2
                    collage_height = height * 2
                    collage = Image.new('RGBA', (collage_width, collage_height))

                    #Appending Images in the Collage image
                    collage.paste(images[0], (0, 0))
                    collage.paste(images[1], (width, 0))
                    collage.paste(images[2], (0, height))
                    collage.paste(images[3], (width, height))

                    #Saving Collage image
                    collage.save(f"{os.path.join(COLLAGE_PATH.format(FOLDER_NAME=FOLDER),city)}.png")
                    print(f"Exporting Collage File: {os.path.join(COLLAGE_PATH.format(FOLDER_NAME=FOLDER),city)}.png")

                else:
                  print(f"Function: export_final_plotted_graphs_collage() Ended as there is no graph exported/present in {city} folder.")
            print(f"Function: export_final_plotted_graphs_collage() Ended Successfully -> {time.strftime(TIME_FORMAT)}")

        else:
            print(f"Function: export_final_plotted_graphs_collage() Ended as there is no City Available in the folder -> {FOLDER}")

    except Exception as Error:
        print(f"Function: export_final_plotted_graphs_collage() Ended with an Error -> {Error}")

def run():

    print(f"EXECUTION STARTED AT: {time.strftime(TIME_FORMAT_COMPLETE)}")
    print("*" * 100)

    csv_dict,xlsx_dict = import_files()
    rcov_dict = extract_rcov(data_list = [csv_dict, xlsx_dict])
    export_plotted_graphs(data_list = [csv_dict,xlsx_dict], rcov_data = rcov_dict)
    export_final_plotted_graphs_collage()

    print("*" * 100)
    print(f"EXECUTION ENDED AT: {time.strftime(TIME_FORMAT_COMPLETE)}")

if __name__ == "__main__":
    run()