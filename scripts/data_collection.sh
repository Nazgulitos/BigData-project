echo "Data Collection is starting..."

echo "Cleaning data folder"
rm -rf data
mkdir -p data

# Download .csv file from Kaggle
kaggle datasets download -d rulyjanuarfachmi/domesticusairflight2016-2018 -p data/

# Unzip the downloaded dataset
unzip data/domesticusairflight2016-2018.zip -d data/

# Move the combined_data.csv file to the data folder
cp data/combine_files.csv data/

#Run the data preprocessing script
python3 scripts/data_preprocess.py

# Delete the combined_data.csv file after preprocessing
rm data/combine_files.csv

# Clean up unnecessary files
rm -rf data/domesticusairflight2016-2018
rm data/domesticusairflight2016-2018.zip

echo "Data Collection is done!"