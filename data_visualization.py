import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
import re

def visualize_all_demand_barplots(input_dir=".", output_dir="visualizations"):
    # Create output folder if not exists
    os.makedirs(output_dir, exist_ok=True)

    # Prepare log
    log_entries = []

    # Find all CSV files in the input directory
    csv_files = glob.glob(os.path.join(input_dir, "region_score_*.csv"))
    if not csv_files:
        print("No CSV files found.")
        return

    for csv_file in csv_files:
        try:
            file_name = os.path.basename(csv_file)

            # Extract concept and date using regex
            match = re.match(r"region_score_(.+)_(\d{8})\.csv", file_name)
            if not match:
                print(f"Skipping {file_name}: filename format not recognized.")
                log_entries.append([file_name, "", "", "N/A", "Skipped - Invalid Filename"])
                continue

            concept_name = match.group(1)
            date_str = match.group(2)

            # Load dataset
            df = pd.read_csv(csv_file)

            # Check required columns
            if not {'region', 'region_score'}.issubset(df.columns):
                print(f"Skipping {file_name}: missing required columns.")
                log_entries.append([file_name, concept_name, date_str, "N/A", "Skipped - Missing Columns"])
                continue

            # Sort by region_score
            region_stats = df[['region', 'region_score']].sort_values(by='region_score', ascending=False)

            # Plot horizontal bar chart
            plt.figure(figsize=(12, max(8, len(region_stats) * 0.3)))  # Dynamic height
            plt.barh(region_stats['region'], region_stats['region_score'], color='skyblue')
            plt.xlabel('Region Score')
            plt.ylabel('Region')
            plt.title(f'Region Demand Score - {concept_name} ({date_str})')
            plt.gca().invert_yaxis()

            # Save figure with concept and date
            output_path = os.path.join(output_dir, f"barplot_{concept_name}_{date_str}.png")
            plt.tight_layout()
            plt.savefig(output_path)
            plt.close()

            print(f"[OK] {file_name} → {output_path}")
            log_entries.append([file_name, concept_name, date_str, output_path, "Success"])

        except Exception as e:
            print(f"[ERROR] {file_name}: {e}")
            log_entries.append([
                file_name,
                concept_name if 'concept_name' in locals() else '',
                date_str if 'date_str' in locals() else '',
                "N/A",
                f"Error: {e}"
            ])

    # Save log as CSV
    log_df = pd.DataFrame(log_entries, columns=['CSV_File', 'Concept', 'Date', 'Output_Image', 'Status'])
    log_path = os.path.join(output_dir, "processing_log.csv")
    log_df.to_csv(log_path, index=False)
    print(f"\nProcessing log saved to {log_path}")


# Run
if __name__ == "__main__":
    visualize_all_demand_barplots(input_dir=".", output_dir="visualizations")
