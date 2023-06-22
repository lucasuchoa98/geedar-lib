import os
import sys
from shutil import copyfile

import typer

from typer import Argument, run
from rich.console import Console
import pandas as pd

from utils import unfoldProcessingCode
from geedar import specificDatesRetrieval, loadInputDF

app = typer.Typer()

running_modes = [
    "1 (specific dates)", "2 (date ranges)", 
    "3 (database update)", "4 (database overwrite)", 
    "5 (estimation overwrite)"
    ]

console = Console()

@app.command()
def main(input_path:str, 
         output:str='', 
         running_mode:str='', 
         aoi_path:str='', 
         aoi_radius:int=1000, 
         append_mode='', 
         time_window:int=2, 
         processing_code=list[int]):

    os.chdir(os.path.realpath(sys.path[0]))

    if running_mode == "":
        if (not input_path[-3:] == ".db") and (not input_path[-4:] == ".csv") and (not input_path[-4:] == ".kml"):
            raise Exception("For the running mode to be automatically determined, the input file's extension must be '.csv' (running mode 1 or 2), '.kml' (running mode 2) or '.db' (running mode 3).")
        
        if input_path[-3:] == ".db":
            running_mode = 3
        
        else:
            # Set running_mode to 0 and determine it later in the function 'loadInputDF', where it will be set to 1 or 2, depending on the CSV's columns.
            running_mode = 0
    else:

        try:
            running_mode = int(running_mode)

        except:
            raise Exception("Running mode must be an integer. Available modes: ".join(running_modes))
        
        else: 
            if not running_mode in range(1, len(running_mode) + 1):
                raise Exception("Unrecognized running mode: '" + str(running_modes) + "'. Available modes: ".join(running_modes))
            
    try:
        splittedPath = os.path.split(input_path)
        input_dir = splittedPath[0]
        if input_dir == "":
            input_dir = "./"
        input_file = splittedPath[1]
    except:
        raise Exception("Unrecognized input file path: '" + input_path + "'.")  
    
    ### Determine the running mode.
    if running_mode == "":
        if (not input_path[-3:] == ".db") and (not input_path[-4:] == ".csv") and (not input_path[-4:] == ".kml"):
            print("!")
            raise Exception("For the running mode to be automatically determined, the input file's extension must be '.csv' (running mode 1 or 2), '.kml' (running mode 2) or '.db' (running mode 3).")
        if input_path[-3:] == ".db":
            running_mode = 3
        else:
            # Set running_mode to 0 and determine it later in the function 'loadInputDF', where it will be set to 1 or 2, depending on the CSV's columns.
            running_mode = 0
    else:
        try:
            running_mode = int(running_mode)
        except:
            print("!")
            raise Exception("Running mode must be an integer. Available modes: ".join(running_modes))


    if running_mode < 3:
        # Confirm the existence of the input file.
        if not os.path.isfile(input_path) and input_file != "*.kml":
            print("!")
            raise Exception("File not found: '" + input_path + "'.")

        ## Unfold the processing code into the IDs of the product, the image processing algorithm, estimation algorithm and reducer.
        processing_codes, product_ids, img_proc_algos, estimation_algos, reducers = unfoldProcessingCode(processing_code)
        print(processing_codes, product_ids, img_proc_algos, estimation_algos, reducers)
        nProcCodes = len(processing_codes)

        output_path = output

        if output_path == "":
            output_dir = input_dir
            if input_file == "*.kml":
                output_file = "kml_result.csv"
            else:
                output_file = input_file[:-4] + "_result.csv"
            output_path = os.path.join(output_dir, output_file)
        else:
            try:
                splittedPath = os.path.split(output_path)
                output_dir = splittedPath[0]
                output_file = splittedPath[1]
            except:
                print("!")
                raise Exception("Unrecognized output file path: '" + output_path + "'.")  
            if output_dir == "":
                output_dir = input_dir #"./"
                output_path = os.path.join(output_dir, output_file)
            elif not os.path.exists(output_dir):
                print("!")
                raise Exception("Directory not found: '" + output_dir + "'.")
        # Check for preexisting file:
        if os.path.isfile(output_path):
            copyfile(output_path, output_path + ".bkp")
            print("(!) Output file already existed, so a backup was created: '" + output_file + ".bkp'.")
        
        ## Area of Interest (AOI) method:
        if aoi_radius != "":
            try:
                aoi_radius = int(aoi_radius)
            except:
                print("!")
                raise Exception("The 'aoi_radius' must be a number greater than zero.")
            if aoi_radius <= 0:
                print("!")
                raise Exception("The 'aoi_radius' must be a number greater than zero.")
        elif not aoi_path in ["", "False", "0"]:
            aoi_mode = "kml" 
        
        ## Time window parameter:
        try:
            time_window = int(time_window)
        except:
            print("!")
            raise Exception("The 'time window' must be an integer greater or equal to zero.")
        if time_window < 0:
            print("!")
            raise Exception("The 'time window' must be an integer greater or equal to zero.")
        
        ## Append mode:
        if not append_mode in ["", "False", "0"]:
            append_mode = True

    # Retrieve data according to the running mode:
    if running_mode < 3:
        loadInputDF(running_mode=running_mode, input_file=input_file, input_path=input_path, input_dir=input_dir)
        resultDF = specificDatesRetrieval()
        if resultDF is None:
            print("No results to be saved.")
        else:
            # Save results.
            print("Saving...")
            try:
                resultDF.to_csv(output_path, index = False)
            except Exception as e:
                print("(!) Failed to save the results to '" + output_path + "'.")
                print(e)
            else:
                print("Results saved to file '" + output_path + "'.")
    elif running_mode in [3,4,5]:
        #databaseUpdate()
        print("Modo ainda não suportado")


if __name__ == '__main__':
    app()