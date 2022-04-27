import pandas as pd
import ssl
import re
import os
ssl._create_default_https_context = ssl._create_unverified_context


def load_and_clean(url):
    """ Loads and cleans the Panamerican Games dataset from a Wikipedia URL and returns the clean dataframe """

    df_panam = pd.read_html(url)[0]
    #df_panam = pd.read_html('https://es.wikipedia.org/wiki/Anexo:Medallero_de_los_Juegos_Panamericanos')[0]
    df_panam.rename(columns = {"País": "Nation", "Oro": "Gold", "Plata": "Silver", "Bronce": "Bronze"}, inplace = True)
    df_panam.set_index('Nation', drop = True, inplace = True)
    df_panam.drop("Puesto", 1, inplace = True)
    df_panam = df_panam[:-1]
    df_panam.to_csv("clean_data/panamerican_medals.csv", mode='w+')

    return df_panam

def build_sports_df(country, n):
    """ Does some basic cleaning and returns a structured data frame for medals. """
    
    url = 'https://es.wikipedia.org/wiki/'
    if ' ' in country:
        country = country.replace(" ", "_")
    url = url + country + '_en_los_Juegos_Panamericanos'
    
    df = pd.read_html(url)[n]
    df.set_axis(['Sport', 'Gold', 'Silver', 'Bronze', 'Total'], axis = 1, inplace = True)
    df = df[:-1]
    df.set_index('Sport', drop = True, inplace = True)
    
    for column in df:
        df[column] = df[column].astype(int)
    
    return df

def add_dataframes(df, others):
    """ Sums the values of several dataframes. Takes an original dataframe (df) and an array of dataframes to add (others) """

    for other in others:
        df = df.add(other, fill_value = 0)
    
    return df

def clean_country_name(country_name, n = 0):
    """ Fixes common Wikipedia string issues """
    
    country_name = country_name.strip(u'\u200b')
    if (re.match('.*?([0-9]+)$', country_name)):
        country_name = re.sub('[0-9]+$','', country_name)
    if(re.match('.*\[.*\]$', country_name)):
        country_name = country_name[:-n]
    if(re.match('.*\(.*\)$', country_name)):
        country_name = re.sub('\(.*\)$','', country_name).strip()

    return country_name

def return_latest_data(df_gdp):
    """ Returns latest data from GDP dataframe. If no data exists for 2019, returns latest available """
    
    n = 2019
    while n >= 1960:
        if pd.isnull(df_gdp[str(n)]) :
            n = n - 1
        else:
            return df_gdp[str(n)]
    

def load_and_clean_indicators(filename, indicator):
    """ Builds path and returns clean dataframe, with passed indicator """
    
    path = 'data/' + filename
    df = pd.read_csv(path, skiprows = 4)
    df[indicator] = df.apply(return_latest_data, axis = 1)
    df = df.rename(columns={'Country Name': 'País'})
    df = df[['País', indicator]]
    df.dropna(inplace = True)
    
    return df

def load_and_clean_corruption_data(url):
    """ Loads and cleans the Corruption Perception Index dataset from a Wikipedia URL and returns the clean dataframe  """

    #df_corruption = pd.read_html()[2]
    df_corruption = pd.read_html(url)[2]
    df_corruption = df_corruption[['País o territorio', '2019[11]​']]
    df_corruption.columns = df_corruption.columns.droplevel()
    df_corruption = df_corruption[['País o territorio', 'Pts.']]
    df_corruption = df_corruption.rename(columns={'País o territorio': 'País', 'Pts.': 'IPC'})

    return df_corruption

def load_and_clean_lima_2019(url):
    """ Loads and cleans the Lima 2019 Panamerican Games dataset from a Wikipedia URL and returns the clean dataframe  """

    df_lima_2019 = pd.read_html(url)[18] 
    df_lima_2019 = df_lima_2019[['País', 'Total']]
    df_lima_2019['País'] =  df_lima_2019['País'].apply(clean_country_name)

    return df_lima_2019

def create_female_medalists_df(df):
    """ Creates dataframe with proportion of female to male medallists  """

    df = df[['Año', 'Sexo']]
    female_medallists = df[df['Sexo'] == 'Femenino'].groupby(by = 'Año').count()['Sexo']
    male_medallists = df[df['Sexo'] == 'Masculino'].groupby(by = 'Año').count()['Sexo']
    df = pd.concat([male_medallists.rename('Medallistas Hombres'), female_medallists.rename('Medallistas Mujeres')], axis = 1)
    df = df.fillna(0)
    df['Proporcion Mujeres'] = df['Medallistas Mujeres'] / (df['Medallistas Hombres'] + df['Medallistas Mujeres'])
    df['Proporcion Hombres'] = df['Medallistas Hombres'] / (df['Medallistas Hombres'] + df['Medallistas Mujeres'])

    return df

def main():

    outdir = './clean_data'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    
    # Load Guatemala by sport
    df_guatemala_sport = build_sports_df('Guatemala', 2)

    # Load El Salvador by sport
    df_el_salvador_sport = build_sports_df('El Salvador', 2)

    # Load Honduras by sport
    df_honduras_sport = build_sports_df('Honduras', 1)

    # Load Nicaragua by sport
    df_nicaragua_sport = build_sports_df('Nicaragua', 1)

    # Load Costa Rica by sport
    df_costa_rica_sport = build_sports_df('Costa Rica', 1)

    # Combine by sport
    df_sport = add_dataframes(df_guatemala_sport, [df_el_salvador_sport, df_honduras_sport, df_nicaragua_sport, df_costa_rica_sport])
    df_sport.Total = df_sport.Total.astype(int)
    df_sport.reset_index(inplace = True)
    df_sport = pd.melt(df_sport, id_vars = ['Sport'], value_vars = ['Gold', 'Silver', 'Bronze'])
    df_sport = df_sport.rename(columns={'variable': 'Medal', 'value': 'Number'})
    df_sport.to_csv("clean_data/sport_medals.csv", mode='w+')

    # Load and save athletes from manually created dataset
    athletes = pd.read_excel("data/athletes_sorted.xlsx") 
    athletes.to_csv("clean_data/athletes.csv", mode='w+')

    # Load GDP data
    df_gdp = load_and_clean_indicators('PIB.csv', 'PIB')

    # Load population data
    df_population = load_and_clean_indicators('poblacion.csv', 'Poblacion')

    # Load Gini index data
    df_gini = load_and_clean_indicators('Gini.csv', 'Gini')

    # Load Lima 2019 Medals data
    df_lima_2019 = load_and_clean_lima_2019('https://es.wikipedia.org/wiki/Juegos_Panamericanos_de_2019#Medallero')

    # Load Corrpution Index data
    df_corruption = load_and_clean_corruption_data('https://es.wikipedia.org/wiki/%C3%8Dndice_de_percepci%C3%B3n_de_corrupci%C3%B3n')

    # Merge them
    df = df_population.merge(df_gdp, on = 'País', how = 'left')
    df = df.merge(df_gini, on = 'País', how = 'left')
    df = df.merge(df_corruption, on = 'País', how = 'left')
    df['País'] = df['País'].replace(['Saint Kitts y Nevis','Islas Vírgenes (EE.UU.)', 'Suriname'],['San Cristóbal y Nieves','Islas Vírgenes de los Estados Unidos','Surinam'])

    # Merge all into df_corr, clean, and save
    df_corr = df_lima_2019.merge(df, on = 'País', how = 'left')
    df_corr.rename(columns={'Total': 'Medallas'}, inplace = True)
    df_corr = df_corr[:-1]
    df_corr.to_csv("clean_data/correlation.csv", mode='w+')

    # Load and save manually created normalized dataset
    normalized_sports = pd.read_excel('data/sports_normalized.xlsx')[['Deporte', 'Medallas Ganadas por CA', 'Medallas Totales', 'Proporcion']][:-1]
    normalized_sports.to_csv("clean_data/sports_normalized.csv", mode='w+')

    # Load and save manually created games dataset
    games = pd.read_excel('data/games.xlsx')
    games.to_csv("clean_data/games.csv", mode='w+')

    # Load and save female medallists dataset
    female_medallists = create_female_medalists_df(athletes)
    female_medallists.to_csv("clean_data/female_medallists.csv", mode='w+')

    print("Success! Check out clean_data on the parent directory")

if __name__=="__main__":
    main()