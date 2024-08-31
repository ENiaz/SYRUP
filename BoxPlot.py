# import pandas
import pandas as pd
# import matplotlib
import matplotlib.pyplot as plt
# import seaborn
import seaborn as sns



# dataset = pd.read_csv(r'AllDistribution.csv')
dataset = pd.read_csv(r'PC.csv')



# df = dataset[dataset['Methods'].isin(['W2V','RDF2Vec','RuleBased'])]


# sns.boxplot(y='values', x='domain', 
#                  data=df, 
#                  palette="colorblind",
#                  hue='Methods')
sns.boxplot(y='PC', x='Domain', 
                 data=dataset, 
                 palette="Blues", width=0.3)

# plt.ylabel('Distribution of Similarity Values')
# plt.xlabel('Domains in Wikidata')
plt.ylabel('PC')
plt.xlabel('Domain')

plt.show()
# plt.savefig('DistBoxPlot.pdf', dpi=100)
plt.savefig('PC.pdf', dpi=100)