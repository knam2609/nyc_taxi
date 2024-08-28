[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Yi0Zbe2y)
# MAST30034 Project 1 README.md
- Name: `Khanh Nam Nguyen`
- Student ID: `1367184`

## Student Instructions
You **must** write up `README.md` for this repository to be eligable for readability marks.

1. Students must keep all Jupyter Notebooks in the `notebooks` directory.
2. Students must keep all `.py` scripts under the `scripts` directory. These can include helper functions and modules with relevant `__init__.py`
3. Students must store all raw data downloaded (using a Python script) in the `data/raw` folder. This will be in the `.gitignore` so **do not upload any raw data files whatsoever**.
4. Students must store all curated / transformed data in the `data/curated` folder. This will be in the `.gitignore` so **do not upload any raw data files whatsoever**. We will be running your code from the `scripts` directory to regenerate these.
5. All plots must be saved in the `plots` directory.
6. Finally, your report `.tex` files must be inside the `report` directory. If you are using overleaf, you can download the `.zip` and extract it into this folder.
7. Add your name and Student ID to the fields above.
8. Add your relevant `requirements.txt` to the root directory. If you are unsure, run `pip3 list --format=freeze > requirements.txt` (or alternative) and copy the output to the repository.
9. You may delete all `.gitkeep` files if you really want to. These were used to ensure empty directories could be pushed to `git`.
10. When you have read this, delete the `Student Instructions` section to clean the readme up.

Remember, we will be reading through and running your code, so it is in _your best interest_ to ensure it is readable and efficient.

## Instruction for Codes
**Research Goal:** Comparing Uber and Yellow Taxi to understand why high-tech transportation is now superior.
**Timeline:** The timeline for the research area is June 2023 - November 2023.

To run the pipeline, please visit `notebooks` and run the files in order:
1. `Download_Data.ipynb`: This downloads the data needed from TLC website to `data/landing`
2. `Inspect_Landing_Data.ipynb`: This notebook is to briefly go through data's content before cleaning
3. `Clean_Data.ipynb`: This cleans the data and save them into `data/raw`
4. `Analyze_{Yellow_Taxi, Uber}.ipynb`: These analyze the data and draw plots
5. `Visualise_Spatial_{Yellow, Uber}_Data.ipynb`: These draw interactive maps to show how geography effects data's attributes
6. `Weather_Data.ipynb`: This download, process and join weather data to curated datasets
6. `Model_{Yellow, Uber}.ipynb`: These use Linear Regression to predict future data based on past data

## Warning
In every notebooks, there is the first cell which I use to adjust python path to the notebook, please delete it if you do not need it or change it into your own path.