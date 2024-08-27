import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import DataFrame
import pandas

def scatter_plot(df: DataFrame, x_axis: str, y_axis: str, type: str, output_path: str):
    """Scatter plot data from 2 columns from PySpark Dataframe"""
    pandas_df = df.select(x_axis, y_axis).toPandas()
    plt.figure(figsize=(10, 5))
    plt.scatter(pandas_df[x_axis], pandas_df[y_axis], alpha=0.5)
    plt.title(f'Scatter Plot of {x_axis} vs {y_axis} of {type.upper()}')
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)
    plt.grid(True)
    plt.tight_layout()  # Adjusts plot to ensure everything fits without overlap
    # Save plot to file
    plot_file = f"{output_path}{x_axis}_vs_{y_axis}_histogram.png"
    print(plot_file)
    plt.savefig(plot_file)
    plt.close()
    plt.show()

def plot_box(df: DataFrame, column: str):
    """
    Generates a box plot for a specified column in a pandas DataFrame using Seaborn.
    """
    pandas_df = df.select(column).toPandas()
    pandas_df = pandas_df.sample(n=10000, random_state=42)  # Sample 10,000 records
    plt.figure(figsize=(10, 6))
    sns.boxplot(y=pandas_df[column])
    plt.ylim([0, 100])
    plt.title(f'Box Plot of {column}')
    plt.grid(True)
    plt.show()

def plot_histogram(df: DataFrame, column: str, type: str, output_path: str, bins=None):
    """
    Generates a histogram for a specified column in a pandas DataFrame using Seaborn.
    """
    pandas_df = df.select(column).toPandas()
    pandas_df = pandas_df.sample(n=10000, random_state=42)  # Sample 10,000 records
    plt.figure(figsize=(10, 6))
    plt.hist(pandas_df[column], bins=bins, color='blue', edgecolor='black')
    x_min = min(pandas_df[column].min(), 0)
    x_max = pandas_df[column].max() + 10
    print(x_min, x_max)
    plt.xlim([x_min, x_max])
    plt.title(f'Histogram of {column.upper()} of {type.upper()}')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.tight_layout()  # Adjusts plot to ensure everything fits without overlap
    # Save plot to file
    plot_file = f"{output_path}{column}_histogram.png"
    print(plot_file)
    plt.savefig(plot_file)
    plt.close()

def plot_correlation_heatmap(df: DataFrame, columns: list[str], type: str, output_path: str, date: str, figsize=(8, 6), cmap='coolwarm'):
    """
    Plots a correlation heatmap for a specified subset of DataFrame columns.
    """
    # Select the columns from the DataFrame
    selected_columns = df.select(columns).toPandas()
    
    # Compute the correlation matrix
    corr_matrix = selected_columns.corr()
    
    # Plotting the heatmap
    plt.figure(figsize=figsize)
    heat_map = sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap=cmap, cbar=True)
    plt.title(f'Correlation Matrix of {type.upper()} in {date}')
    plt.tight_layout()  # Adjusts plot to ensure everything fits without overlap
    # Save plot to file
    plot_file = f"{output_path}{date}.png"
    print(plot_file)
    plt.savefig(plot_file)
    plt.close()  # Close the figure to free up memory