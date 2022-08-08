import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from itertools import product
from collections import defaultdict

def get_costs_from_df(df):
    return df["config_point_result"].loc[:, ['execution_time', 'cost_to_run']]
    
# Pareto front is defined as the set of points where one cost cannot be greater without lowering the other one
# Ideally this function will return the indexes of the array so that we can later referrence the original dataframe if we need to 
def find_pareto_front(costs_np_arr):
    n_samples = costs_np_arr.shape[0]
    in_p_front = np.full(n_samples, True)
    for i, c in enumerate(costs_np_arr):
        if in_p_front[i]:
            in_p_front[in_p_front] = np.any(costs_np_arr[in_p_front] < c, axis=1)
            in_p_front[i] = True

    return in_p_front

def get_pareto_front(df, sort_col = ("config_point_result", "execution_time")):
    costs_df = get_costs_from_df(df)
    pareto_indeces = find_pareto_front(costs_df.to_numpy())
    pareto_df = df.loc[pareto_indeces].sort_values(by=[sort_col])
    return pareto_df

def plot_pareto(df, ax=None, pareto_points_color=None, plot_name="", mark_baseline=True, **kwargs):
    costs_df = get_costs_from_df(df)
    pareto_indeces = find_pareto_front(costs_df.to_numpy())

    if ax == None:
        fig, ax = plt.subplots(figsize=(7, 5))
    
    scatter_points_c = sns.color_palette()[0]
    pareto_points_c = sns.color_palette()[1]
    if pareto_points_color:
        pareto_points_c = pareto_points_color
    sns.scatterplot(data=costs_df, x="execution_time", y="cost_to_run", color=scatter_points_c, ax=ax)

    # There's only point in the pareto front
    label = "Pareto front"
    if "label" in kwargs:
        label = kwargs["label"]
    if len(costs_df[pareto_indeces]) == 1:
        sns.scatterplot(data=costs_df[pareto_indeces], x="execution_time", y="cost_to_run", color=pareto_points_c, label=label, ax=ax)
    else:
        sns.lineplot(data=costs_df[pareto_indeces], x="execution_time", y="cost_to_run", color=pareto_points_c, label=label, marker='o', ax=ax)
    # Plot baseline
    if mark_baseline:
        sns.scatterplot(data=costs_df[:1], x="execution_time", y="cost_to_run", color="red", marker="X", label="Baseline", edgecolor='black', s=100, ax=ax)    

    if plot_name:
        ax.set_title(plot_name)

def get_percentages(df):
    percentages = df["function_times"].divide(df["config_point_result"]["execution_time"], axis=0)
    return percentages

def get_worst_point(df):
    return df.loc[df['config_point_result']['execution_time'] == df['config_point_result']['execution_time'].max()]

def get_median(df):
    attempts_per_config = get_attempt_n(df)
    df = df.groupby(df.index // attempts_per_config).median()
    df = df.drop(['attempt_n'], axis=1, level=1)
    return df

def get_attempt_n(df):
    return df['config_point_result']['attempt_n'].nunique()

def get_overhead(df):
    new_df = df.copy()
    new_df["overhead"] =  df["config_point_result"]["execution_time"] - new_df["function_times"].sum(axis=1)
    return new_df

def plot_per_function_time(df):
    function_names = df["function_resource_map"].columns
    n_functions = len(function_names)
    fig, axs = plt.subplots(n_functions, 2, figsize=(8, 4*n_functions))

    pd.concat([df["function_times"], df["config_point_result"]["execution_time"]], axis=1)
    for i, function_name in enumerate(function_names):
        ax = axs[i, 0]
        new_df = pd.concat([df["function_resource_map"][function_name], df["function_times"][function_name], df["config_point_result"]["execution_time"]], axis=1)
        new_df.columns = ['cpu', 'time_in_function', 'total_execution_time']
        sns.boxplot(x="cpu", y="time_in_function", data=new_df, ax=ax).set(title=function_name)
        
        ax = axs[i, 1]
        sns.boxplot(x="cpu", y="total_execution_time", data=new_df, ax=ax).set(title=function_name)

    fig.tight_layout()

def plot_per_function_pareto_front(df):
    function_names = df["function_resource_map"].columns
    n_functions = len(function_names)
    fig, axs = plt.subplots(1, n_functions, figsize=(4*n_functions, 4))

    base_line_row = df.iloc[:1]
    print("Remember, assuming first row as baseline.")
    for i, function_name in enumerate(function_names):
        ax = axs[i]
        temp_df = df.loc[df["function_resource_map"][function_name] != 1.0]
        relevant_rows = pd.concat([base_line_row, temp_df])
        f_df = pd.concat([relevant_rows["function_resource_map"][function_name], relevant_rows["function_times"][function_name]], axis=1)
        f_df.columns = ['cpu', 'execution_time']
        f_df["cost_to_run"] = f_df['cpu'] * f_df['execution_time']
        f_df.columns = pd.MultiIndex.from_arrays([["f_map", "config_point_result", "config_point_result"], f_df.columns])
        
        ax.set_title(function_name)
        plot_pareto(f_df, ax)

    fig.tight_layout()

def combine_per_function_pareto_front(df):
    function_names = df["function_resource_map"].columns
    resource_per_function = [df["function_resource_map"][func_name].unique() for func_name in function_names]
    f_resource_grid = [dict(zip(function_names, values)) for values in product(*resource_per_function)]

    cache_exec_time = defaultdict(dict)
    base_line_row = df.iloc[:1]
    for i, f_name in enumerate(function_names):
        for cpu in resource_per_function[i]:
            if cpu == 1.0:
                wanted_row = base_line_row
            else:
                wanted_row = df.loc[df["function_resource_map"][f_name] == cpu]
            cache_exec_time[f_name][cpu] = wanted_row["function_times"][f_name].values[0]

    df_list = []
    for f_resources in f_resource_grid:
        row = [*f_resources.values()]
        exec_time = 0
        cost = 0
        for f_name, cpu in f_resources.items():
            f_exec = cache_exec_time[f_name][cpu]
            exec_time += f_exec
            cost += cpu * f_exec

        row.append(exec_time)
        row.append(cost)
        df_list.append(row)

    columns = [*[("function_resource_map", func_name) for func_name in function_names], ("config_point_result", "execution_time"), ("config_point_result", "cost_to_run")]
    combined_df = pd.DataFrame(df_list, columns=pd.MultiIndex.from_tuples(columns))
    return combined_df

def plot_compare_pareto_fronts(dfs, dfs_names, title=""):
    if len(dfs) != len(dfs_names):
        raise ValueError("Number of dfs does not match with number of dfs")
    n_dfs = len(dfs)

    colors = sns.color_palette()
    fig, ax = plt.subplots(1, n_dfs+1, figsize=(7*(n_dfs+1), 5), sharex='all', sharey='all')
    for i, df in enumerate(dfs):
        plot_pareto(df, ax=ax[i], pareto_points_color=colors[i+1], plot_name=dfs_names[i], mark_baseline=False)
        pf = get_pareto_front(df)
        plot_pareto(dfs[0][:1], ax=ax[i], label="", mark_baseline=True)

        plot_pareto(pf, ax=ax[-1], pareto_points_color=colors[i+1], plot_name="Comparison", label=dfs_names[i], mark_baseline=False)
        ax[i].yaxis.set_tick_params(labelleft=True)
    # Also apply it to the comparison plot
    ax[-1].yaxis.set_tick_params(labelleft=True)
    plot_pareto(dfs[0][:1], ax=ax[-1], label="", mark_baseline=True)


    if title == "":
        title = "Comparing pareto fronts"


    fig.suptitle(title)

def make_per_function_from_grid_search(grid_df):
    function_names = grid_df["function_resource_map"].columns
    n_functions = len(function_names)
    resource_per_function = [grid_df["function_resource_map"][func_name].unique() for func_name in function_names]
    step = 1
    index = 1
    interesting_indexes = [0]
    for i in range(n_functions):
        cpu_tested = len(resource_per_function[i]) - 1 #remove baseline
        for _ in range(cpu_tested):
            interesting_indexes.append(index)
            index += step
        step *= (cpu_tested + 1) #restore baseline
    return grid_df.iloc[interesting_indexes]

       
def read_df(csv_file, ignore_functions=None, median=True):
    df = pd.read_csv(csv_file, header=[0, 1])
    if ignore_functions:
        df = df.rename(columns={"cost_to_run": "original_cost"}, level=1)
        df.loc[:,('config_point_result', 'cost_to_run')] = df["config_point_result"]["original_cost"]
        for func_name in ignore_functions:
            if func_name not in df["function_resource_map"]:
                continue
            df = df[df["function_resource_map"][func_name] == 1.0]
            df.loc[:,('config_point_result', 'cost_to_run')] = df["config_point_result"]["cost_to_run"] - df["function_times"][func_name]
        df = df.drop(ignore_functions, axis=1, level=1, errors='ignore') # ignore error if column does not exist
    if median:
        df = get_median(df)
    return df