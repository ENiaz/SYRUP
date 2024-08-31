#!/usr/bin/python -u

# force matplotlib agg backend
import matplotlib
matplotlib.use("agg")
import matplotlib.pyplot as plt
from scipy.ndimage.filters import gaussian_filter1d
from scipy.interpolate import UnivariateSpline

import os
import numpy as np
from argparse import ArgumentParser



color_map = None
print_title = None
gaussian_enable = None
gaussian_sigma = None
interpolation_enable = None
interpolation_segments = None
interpolation_order = None


def get_synonyms_set(synonyms_fn):
    synonyms_file = open(synonyms_fn, "r")
    synonyms_lines = synonyms_file.readlines()[1:]
    synonyms_set = set(map(lambda x: frozenset(map(lambda y: int(y), x.split())), synonyms_lines))
    synonyms_file.close()
    return synonyms_set
    
def calc_true_positives(classified_synonyms_set, ground_truth_synonyms_set):
    return set(filter(lambda x: x in ground_truth_synonyms_set, classified_synonyms_set))
    
def calc_precision_recall(classified_synonyms_set, ground_truth_synonyms_set):
    num_true_positives = len(calc_true_positives(classified_synonyms_set, ground_truth_synonyms_set))
    num_classified_positives = len(classified_synonyms_set)
    num_ground_truth_positives = len(ground_truth_synonyms_set)
    precision = (float(num_true_positives) / float(num_classified_positives)) if num_classified_positives != 0 else 0.0
    recall = (float(num_true_positives) / float(num_ground_truth_positives)) if num_ground_truth_positives != 0 else 0.0
    return precision, recall


    if evaluation:
        evaluation_l1 = []
        synonyms = get_synonyms_set(fn_synonyms_id)
        def precision_recall(l):
            precision, recall = calc_precision_recall(l, synonyms)
            return [precision, recall]
        def save_evaluation(evaluation, file_name):
            evaluation_file = open(file_name, "w")
            evaluation_file.writelines(map(lambda x: str(x[0]) + "\t" + str(x[1]) + "\n", evaluation))
            evaluation_file.close()

    if evaluation:
        evaluation_l1.append(precision_recall(l1))
        
        save_evaluation(evaluation_class_l1, os.path.join(evaluation_dir, "evaluation_l1.txt"))
        


def plot_evaluation(file_name, xlabel, ylabel, title, \
        xlim=[0.0, 1.0], ylim=[0.0, 1.0], xtick_step=0.1, ytick_step=0.1, \
        **values_by_model):
    # prepare plotting
    get_precision = lambda x: list(map(lambda y: float(y[0]), x))
    get_recall = lambda x: list(map(lambda y: float(y[1]), x))
    get_valid_num = lambda x: len(list(filter(lambda y: y > 0.0, x)))
    plt.clf()
    plt.xlim(xlim)
    plt.ylim(ylim)
    
    # get color map
    cmap = plt.get_cmap(color_map)
    
    if "transd" in values_by_model:
        precision = get_precision(values_by_model["transd"])
        recall = get_recall(values_by_model["transd"])
        if gaussian_enable:
            precision = gaussian_filter1d(precision, sigma=gaussian_sigma)
        if interpolation_enable:
            new_recall = np.linspace(min(recall), max(recall), interpolation_segments)
            f = UnivariateSpline(recall, precision, k=interpolation_order)
            precision = f(new_recall)
            recall = new_recall
        min_idx = len(precision) - min(get_valid_num(precision), get_valid_num(recall))
        plt.plot(recall[min_idx:], precision[min_idx:], \
                color='orange', linestyle='solid', label = "TransD")

    if "transh" in values_by_model:
        precision = get_precision(values_by_model["transh"])
        recall = get_recall(values_by_model["transh"])
        if gaussian_enable:
            precision = gaussian_filter1d(precision, sigma=gaussian_sigma)
        if interpolation_enable:
            new_recall = np.linspace(min(recall), max(recall), interpolation_segments)
            f = UnivariateSpline(recall, precision, k=interpolation_order)
            precision = f(new_recall)
            recall = new_recall
        min_idx = len(precision) - min(get_valid_num(precision), get_valid_num(recall))
        plt.plot(recall[min_idx:], precision[min_idx:], \
                color='blue', linestyle='solid', label = "TransH")

    if "transe" in values_by_model:
        precision = get_precision(values_by_model["transe"])
        recall = get_recall(values_by_model["transe"])
        if gaussian_enable:
            precision = gaussian_filter1d(precision, sigma=gaussian_sigma)
        if interpolation_enable:
            new_recall = np.linspace(min(recall), max(recall), interpolation_segments)
            f = UnivariateSpline(recall, precision, k=interpolation_order)
            precision = f(new_recall)
            recall = new_recall
        min_idx = len(precision) - min(get_valid_num(precision), get_valid_num(recall))
        plt.plot(recall[min_idx:], precision[min_idx:], \
                color='gray', linestyle='solid', label = "TransE")    

    if "complex" in values_by_model:
        precision = get_precision(values_by_model["complex"])
        recall = get_recall(values_by_model["complex"])
        if gaussian_enable:
            precision = gaussian_filter1d(precision, sigma=gaussian_sigma)
        if interpolation_enable:
            new_recall = np.linspace(min(recall), max(recall), interpolation_segments)
            f = UnivariateSpline(recall, precision, k=interpolation_order)
            precision = f(new_recall)
            recall = new_recall
        min_idx = len(precision) - min(get_valid_num(precision), get_valid_num(recall))
        plt.plot(recall[min_idx:], precision[min_idx:], \
                color='violet', linestyle='solid', label = "ComplEx")

    if "NumEmb" in values_by_model:
        precision = get_precision(values_by_model["NumEmb"])
        recall = get_recall(values_by_model["NumEmb"])
        if gaussian_enable:
            precision = gaussian_filter1d(precision, sigma=gaussian_sigma)
        if interpolation_enable:
            new_recall = np.linspace(min(recall), max(recall), interpolation_segments)
            f = UnivariateSpline(recall, precision, k=interpolation_order)
            precision = f(new_recall)
            recall = new_recall
        min_idx = len(precision) - min(get_valid_num(precision), get_valid_num(recall))
        plt.plot(recall[min_idx:], precision[min_idx:], \
                color='red', linestyle='solid', label = "RDF2Vec")

    if "syrup" in values_by_model:
        precision = get_precision(values_by_model["syrup"])
        recall = get_recall(values_by_model["syrup"])
        if gaussian_enable:
            precision = gaussian_filter1d(precision, sigma=gaussian_sigma)
        if interpolation_enable:
            new_recall = np.linspace(min(recall), max(recall), interpolation_segments)
            f = UnivariateSpline(recall, precision, k=interpolation_order)
            precision = f(new_recall)
            recall = new_recall
        min_idx = len(precision) - min(get_valid_num(precision), get_valid_num(recall))
        plt.plot(recall[min_idx:], precision[min_idx:], \
                color='green', linestyle='solid', label = "SYRUP")
    
    # tick frequency
    axes = plt.gca()
    start, end = axes.get_xlim()
    axes.set_xticks(np.arange(start, end, xtick_step))
    start, end = axes.get_ylim()
    axes.set_yticks(np.arange(start, end, ytick_step))
    
    # show grid
    plt.grid(True)
    
    # label and save
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend(loc = "upper right")
    plt.savefig(file_name)

def plot_experiment_evaluation(experiment_dir_without_model_suffix, baseline_fn=None):
    def read_evaluation(file_name):
        evaluation_file = open(file_name, "r")
        values = list(map(lambda x: x.split(), evaluation_file.readlines()))
        evaluation_file.close()
        return values
    
    # check which models exist
    experiment = experiment_dir_without_model_suffix.rstrip("/")
    dirname = os.path.dirname(experiment)
    basename = os.path.basename(experiment)
    transd_fn = experiment + "_transd"
    transh_fn = experiment + "_transh"
    transe_fn = experiment + "_transe"
    complex_fn = experiment + "_complex"
    NumEmb_fn = experiment + "_NumEmb"
    syrup_fn = experiment + "_syrup"
    transd = os.path.exists(transd_fn)
    transh = os.path.exists(transh_fn)
    transe = os.path.exists(transe_fn)
    complex = os.path.exists(complex_fn)
    NumEmb = os.path.exists(NumEmb_fn)
    syrup = os.path.exists(syrup_fn)
    
    transd_values = read_evaluation(os.path.join(transd_fn, \
            "evaluation", "evaluation_l1.txt")) if transd else None
    transh_values = read_evaluation(os.path.join(transh_fn, \
            "evaluation", "evaluation_l1.txt")) if transh else None
    transe_values = read_evaluation(os.path.join(transe_fn, \
            "evaluation", "evaluation_l1.txt")) if transe else None
    complex_values = read_evaluation(os.path.join(complex_fn, \
            "evaluation", "evaluation_l1.txt")) if complex else None
    NumEmb_values = read_evaluation(os.path.join(NumEmb_fn, \
            "evaluation", "evaluation_l1.txt")) if NumEmb else None
    syrup_values = read_evaluation(os.path.join(syrup_fn, \
            "evaluation", "evaluation_l1.txt")) if syrup else None
    file_name = os.path.join(dirname, "PrecisionRecallEval.pdf")
    title = basename + ": Evaluation (L1-norm distance)" \
            if print_title else None
    values_by_model = {}
    if transd:
        values_by_model["transd"] = transd_values
    if transh:
        values_by_model["transh"] = transh_values
    if transe:
        values_by_model["transe"] = transe_values    
    if complex:
        values_by_model["complex"] = complex_values    
    if NumEmb:
        values_by_model["NumEmb"] = NumEmb_values
    if syrup:
        values_by_model["syrup"] = syrup_values
    plot_evaluation(file_name, "RECALL", "PRECISION", title, **values_by_model)



def plot_dbpedia_evaluation():
    # for every dbpedia embedding
    dbpedia_names = "DBpedia_{0}_{1}"
    experiments_fn = "experiments/tf-gpu_1.11.0/"
    percentages = range(10, 60, 10)
    min_occurences = range(200, 2200, 200)
    for percentage in percentages:
        for min_occurence in min_occurences:
            dbpedia_name = dbpedia_names.format(percentage, min_occurence)
            plot_experiment_evaluation(os.path.join(experiments_fn, dbpedia_name))

def main():
    # parse arguments
    parser = ArgumentParser()
    exclusive_group = parser.add_mutually_exclusive_group(required=True)
    exclusive_group.add_argument("-f", "--dbpedia-analysis", action="store_true", \
            default=False, help="Perform plotting for all DBpedia experiments")
    exclusive_group.add_argument("-e", "--experiment", type=str, default=None, \
            help="Plot evaluations of every model for this experiment" \
            + " (directory without model suffix, so this script can" \
            + " get every model to evaluate). If None (not specified)," \
            + " it will evaluate each DBpedia experiment (without any baseline)" \
            + " (Default: None).")
    parser.add_argument("-b", "--baseline", type=str, default=None, \
            help="An optional baseline evaluation file to add to the plots" \
            + " of a specified experiment (ignored with -f) (Default: None).")
    parser.add_argument("-c", "--color-map", type=str, choices=plt.colormaps(), default="nipy_spectral_r", \
            help="The color map to use for plotting multiple curves in one diagram (Default: nipy_spectral_r).")
    parser.add_argument("-t", "--print-title", action="store_true", default=False, \
            help="Print titles in plots (Default: False).")
    parser.add_argument("-g", "--gaussian-enable", action="store_true", default=False, \
            help="Enable gaussian filter for smoothing (Default: False).")
    parser.add_argument("-s", "--gaussian-sigma", type=float, default=2.0, \
            help="The sigma scalar for gaussian filter kernel, use with -g option (Default: 2.0).")
    parser.add_argument("-i", "--interpolation-enable", action="store_true", default=False, \
            help="Enable interpolation for smoothing (Default: False).")
    parser.add_argument("-n", "--interpolation-segments", type=int, default=100, \
            help="The number of equidistant segments along recall axis for interpolation,"
            + " use with -i option (Default: 100).")
    parser.add_argument("-o", "--interpolation-order", type=int, default=15, \
            help="The interpolation order, use with -i option (Default: 15).")
    args = parser.parse_args()
    
    # set parameters
    global color_map
    global print_title
    global gaussian_enable
    global gaussian_sigma
    global interpolation_enable
    global interpolation_segments
    global interpolation_order
    color_map = args.color_map
    print_title = args.print_title
    gaussian_enable = args.gaussian_enable
    gaussian_sigma = args.gaussian_sigma
    interpolation_enable = args.interpolation_enable
    interpolation_segments = args.interpolation_segments
    interpolation_order = args.interpolation_order
    
    # plot for experiment if argument specified,
    # else just plot for dbpedia if -f specified
    if args.dbpedia_analysis:
        print("Plotting for all DBpedia experiments")
        plot_dbpedia_evaluation()
    elif args.experiment:
        print("Plotting for " + args.experiment)
        plot_experiment_evaluation(args.experiment, args.baseline)

if __name__ == "__main__":
    main()

