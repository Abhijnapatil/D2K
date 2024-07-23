import os
import pandas as pd

# Function to load and process a single CSV file
def process_trip_data_file(file_path):
    try:
        # Load CSV into DataFrame
        df = pd.read_csv(file_path)
        
        # Drop rows with any missing values
        df.dropna(inplace=True)
        
        # Convert pickup and drop-off datetime strings to datetime objects
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
        
        # Calculate trip duration in seconds
        df['trip_duration_seconds'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds()
        
        # Calculate average speed in miles per hour
        df['average_speed_mph'] = df['trip_distance'] / (df['trip_duration_seconds'] / 3600)
        
        # Aggregate data to calculate total trips and average fare per day
        df['pickup_date'] = df['pickup_datetime'].dt.date
        daily_summary = df.groupby('pickup_date').agg({
            'fare_amount': ['count', 'mean']
        }).reset_index()
        daily_summary.columns = ['pickup_date', 'total_trips', 'average_fare']
        
        return daily_summary
    
    except Exception as e:
        print(f'Error processing {file_path}: {str(e)}')
        return None

# Main function to process all files in the directory
def process_trip_data(directory):
    processed_data = []
    
    # Iterate through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            print(f'Processing {filename}...')
            
            # Process the file and append the result to processed_data
            result = process_trip_data_file(file_path)
            if result is not None:
                processed_data.append(result)
    
    # Concatenate all daily summaries into a single DataFrame
    if processed_data:
        final_df = pd.concat(processed_data, ignore_index=True)
        return final_df
    else:
        return None

if __name__ == "__main__":
    data_directory = './trip_data_2019/'  # Replace with your actual directory path
    processed_data = process_trip_data(data_directory)
    
    if processed_data is not None:
        # Save the processed data to a CSV file
        processed_data.to_csv('processed_trip_data_2019.csv', index=False)
        print('Processing complete. Processed data saved to processed_trip_data_2019.csv.')
    else:
        print('No data processed or saved due to errors.')
