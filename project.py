# Names:
# Jelle Maas s1822306
# Allesandra Amato s2886561
# Rosalie Voorend s2134861
# Valeria Pinus s2844559

# All libraries and converting everything into utf8 encoding.
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, array_contains, lower

spark = SparkSession.builder.getOrCreate()


# Method for lowering all words in the arrays
def lower_array(array):
	lowercase_array = []
	for i in range(len(array)):
		lowercase_array.append(array[i].lower())
	return lowercase_array


# All hashtags that are used to filter for the Olympic Games
always = ["teamNL", "OS", "Olympics", "GoTeamNL", "OlympicTeamNL", "Goud", "Brons", "Zilver", "Mannen", "Dames",
          "Finale", "NL", "Oranje", "Openingceremony", "Trots", "Heineken"]
alwaysl = lower_array(always)
game12 = ["OS2012", "Zomerspelen", "Londen", "London2012", "Londen2012", "OlympischeSpelen2012", "EpkeZonderland",
          "O2arena", "Londen", "Uk", "UK", "NOS"]
game12l = lower_array(game12)
game14 = ["OlympischeSpelen2014", "Winterspelen" "Sochi", "Sotchi2014", "Sochi2014", "Sotsji2014", "OS2014", "Sotsji",
          "Sotchi", "Sochi", "Kramer", "Bergsma", "Rusland", "OlympischeWinterSpelen2014", "1000M"]
game14l = lower_array(game14)
game16 = ["OlympischeSpelen2016", "Rio2016", "Zomerspelen", "Rio", "Brazil", "Brugge", "RioDeJaneiro", "OS2016"]
game16l = lower_array(game16)
game18 = ["Schulting", "Winterspelen", "Pyeongchang", "OlympischeSpelen2018", "Pyeongchang2018", "Nuis", "Wereldrecord",
          "Shorttrack", "Schaatsen", "10KM", "OS2018", "Collectievissen"]
game18l = lower_array(game18)

# All sport related hashtags, for both Summer and Winter Olympics in English and Dutch
sportss = ["Archery", "Boxing", "Canoe", "Kayak", "Cycling", "Diving", "Equestrian", "Fencing", "Field Hockey",
           "Gymnastics", "Modern Pentathlon", "Pentathlon", "Rowing", "Sailing", "Shooting", "Soccer", "Swimming",
           "Synchronized Swimming", "Table Tennis", "Track and Field", "Triathlon", "Volleyball", "Water Polo",
           "Weightlifting", "Wrestling"]
sportssl = lower_array(sportss)
sportens = ["Boogschieten", "Badminton", "Boksen", "Kano", "Kajak", "Fietsen", "Wielrennen", "Duiken", "Paardrijden",
            "Schermen", "Hockey", "Gymnastiek", "Turnen", "Judo", "Moderne vijfkamp", "vijfkamp", "roeien", "zeilen",
            "schieten", "voetbal", "zwemmen", "gesynchroniseerd zwemmen", "tafeltennis", "pingpong", "taekwondo",
            "tennis", "Triatlon", "Volleybal", "Beach", "Waterpolo", "Gewichtheffen", "Worstelen"]
sportensl = lower_array(sportens)
sportsw = ["Alpine Skiing", "Biathlon", "Bobsled", "Cross Country Skiing", "Curling", "Figure Skating",
           "Freestyle Skiing", "Ice Hockey", "Luge", "Nordic Combined", "Skeleton", "Ski Jumping", "Snowboarding",
           "Speedskating", "Ice Skating", "Skiing"]
sportswl = lower_array(sportsw)
sportenw = ["Alpineskien", "Biatlon", "Bobslee", "Langlaufen", "Curlen", "Kunstschaatsen", "Freestyle Skien",
            "IJshockey", "Rodelen", "Noorse combinatie", "Skispringen", "Snowboarden", "Snelschaatsen", "Schaatsen",
            "Skien"]
sportenwl = lower_array(sportenw)

# All cultural aspects at the Olympic games
culture2012 = ["jamesbond", "queen", "elizabeth", "queenelizabeth", "mrbean", "bean", "rowan", "atkinson",
               "rowanatkinson", "daniel", "craig", "danielcraig", "arcticmonkey", "spicegirls", "edsheeran", "1d",
               "onedirection", "directioner", "harry", "liam", "niall", "louis", "zayn"]
culture2014 = ["tolstoy", "putin", "zwanenmeer", "swanlake", "maria", "sharapova", "mariasharapova"]
culture2016 = ["kygo", "giselebundchen", "gisele", "bundchen", "forrest", "carnaval"]


# Function to union dataframes because Functools library did not work
def unionAllv2(dfs, total=None):
	i = 0
	length = len(dfs)
	been_here = False
	if length > 1:
		while i < length:
			if i + 1 < length and been_here != True:
				total = dfs[i + 1].union(dfs[i])
				i += 2
				been_here = True
			else:
				total = total.union(dfs[i])
				i += 1
		return total
	else:
		return dfs[0]


# Function to retrieve all hashtags from the dataframe
def hashtag_retrieve(df):
	filtered_df = df.select(col("entities")["hashtags"]["text"].alias("hashtags")).filter(size(col("hashtags")) > 0)
	return filtered_df


# Function to retrieve both the hashtags and username of the person who tweeted from the dataframe
def hashtag_user_retrieve(df):
	filtered_df = df.select(col("entities")["hashtags"]["text"].alias("hashtags"),
	                        col("user")["name"].alias("User")).filter(size(col("hashtags")) > 0)
	return filtered_df


# Summer Olympics: 27-12 augustus 2012
dfl1 = spark.read.json("/data/twitterNL/201207/20120727-**.out.gz")  # 27 jul 2012 (opening ceremony day)
dfl2 = spark.read.json("/data/twitterNL/201207/20120728-**.out.gz")  # 28 jul 2012
dfl3 = spark.read.json("/data/twitterNL/201207/20120729-**.out.gz")  # 29 jul 2012
dfl4 = spark.read.json("/data/twitterNL/201207/2012073*-**.out.gz")  # 30-31 jul 2012
dfl5 = spark.read.json("/data/twitterNL/201208/2012080*-**.out.gz")  # 1-9 aug 2012
dfl6 = spark.read.json("/data/twitterNL/201208/20120810-**.out.gz")  # 10 aug 2012
dfl7 = spark.read.json("/data/twitterNL/201208/20120811-**.out.gz")  # 11 aug 2012
dfl8 = spark.read.json("/data/twitterNL/201208/20120812-**.out.gz")  # 12 aug 2012

# Creating one big dataframe with all hashtags in the Olympic Games period
df2012OG = unionAllv2([hashtag_retrieve(dfl1), hashtag_retrieve(dfl2), hashtag_retrieve(dfl3), hashtag_retrieve(dfl4),
                       hashtag_retrieve(dfl5), hashtag_retrieve(dfl6), hashtag_retrieve(dfl7), hashtag_retrieve(dfl8)])

# Winter Olympics: 7-23 februari 2014 (Sochi)
dfs2 = spark.read.json("/data/twitterNL/201402/20140207-**.out.gz")  # 7 feb 2014 (opening ceremony day)
dfs3 = spark.read.json("/data/twitterNL/201402/20140208-**.out.gz")  # 8 feb 2014
dfs4 = spark.read.json("/data/twitterNL/201402/20140209-**.out.gz")  # 9 feb 2014
dfs5 = spark.read.json("/data/twitterNL/201402/2014021*-**.out.gz")  # 10-19 feb 2014
dfs6 = spark.read.json("/data/twitterNL/201402/20140220-**.out.gz")  # 20 feb 2014
dfs7 = spark.read.json("/data/twitterNL/201402/20140221-**.out.gz")  # 21 feb 2014
dfs8 = spark.read.json("/data/twitterNL/201402/20140222-**.out.gz")  # 22 feb 2014
dfs9 = spark.read.json("/data/twitterNL/201402/20140223-**.out.gz")  # 23 feb 2014 (closing ceremony day)

df2014OG = unionAllv2([hashtag_retrieve(dfs2), hashtag_retrieve(dfs3), hashtag_retrieve(dfs4), hashtag_retrieve(dfs5),
                       hashtag_retrieve(dfs6), hashtag_retrieve(dfs7), hashtag_retrieve(dfs8), hashtag_retrieve(dfs9)])

# Summer Olympics: 5-21 augustus 2016
dfr1 = spark.read.json("/data/twitterNL/201608/20160805-**.out.gz")  # 5 aug 2016 (opening ceremony day)
dfr2 = spark.read.json("/data/twitterNL/201608/20160806-**.out.gz")  # 6 aug 2016
dfr3 = spark.read.json("/data/twitterNL/201608/20160807-**.out.gz")  # 7 aug 2016
dfr4 = spark.read.json("/data/twitterNL/201608/20160808-**.out.gz")  # 8 aug 2016
dfr5 = spark.read.json("/data/twitterNL/201608/20160809-**.out.gz")  # 9 aug 2016
dfr6 = spark.read.json("/data/twitterNL/201608/2016081*-**.out.gz")  # 10-19 aug 2016
dfr7 = spark.read.json("/data/twitterNL/201608/20160820-**.out.gz")  # 20 aug 2016
dfr8 = spark.read.json("/data/twitterNL/201608/20160821-**.out.gz")  # 21 aug 2016 (closing ceremony day)

df2016OG = unionAllv2([hashtag_retrieve(dfr1), hashtag_retrieve(dfr2), hashtag_retrieve(dfr3), hashtag_retrieve(dfr4),
                       hashtag_retrieve(dfr5), hashtag_retrieve(dfr6), hashtag_retrieve(dfr7), hashtag_retrieve(dfr8)])


# Function that counts the amount of hashtags in a dataframe and sorts in from most to least popular
def count_hashtags(dff):
	df = dff.select(explode(col("hashtags")))
	df2 = df.groupBy(lower(df[0]).alias("hashtags")).count()
	df3 = df2.sort(df2[1].desc())
	return df3


# Function that filters the hashtags for each Olympic Games
def combine(df, spht, sphtl):
	df_array = []
	df1 = []
	df2 = []
	df3 = []
	df4 = []
	for word in always:
		df_array.append(df.select(col("hashtags")).filter(array_contains(col("hashtags"), word)))
		df1 = unionAllv2(df_array)
		df_array = []
	for word in alwaysl:
		df_array.append(df.select(col("hashtags")).filter(array_contains(col("hashtags"), word)))
		df2 = unionAllv2(df_array)
		df_array = []
	for word in spht:
		df_array.append(df.select(col("hashtags")).filter(array_contains(col("hashtags"), word)))
		df3 = unionAllv2(df_array)
		df_array = []
	for word in sphtl:
		df_array.append(df.select(col("hashtags")).filter(array_contains(col("hashtags"), word)))
		df4 = unionAllv2(df_array)
		df_array = []
	return count_hashtags(unionAllv2([df1, df2, df3, df4]))


# Function to return the amount of sport hashtags that are used in the filtered Olympic Games dataframe
def sports_count(df, sportsengl, sportsdutchl):
	dfs = []
	df1 = []
	df2 = []
	for word in sportsengl:
		dfs.append(df.select(col("hashtags"), col("count")).filter(df.hashtags == word))
		df1 = unionAllv2(dfs)
		dfs = []
	for word in sportsdutchl:
		dfs.append(df.select(col("hashtags"), col("count")).filter(df.hashtags == word))
		df2 = unionAllv2(dfs)
	df = unionAllv2([df1, df2])
	df2 = df.sort(df[1].desc())
	return df2


# Function to return the amount of cultural hashtags that are used in the filtered Olympic Games dataframe
def culture_count(df, culturel):
	dfs = []
	for word in culturel:
		dfs.append(df.select(col("hashtags"), col("count")).filter(df.hashtags == word))
	df1 = unionAllv2(dfs)
	df2 = df1.sort(df1[1].desc())
	return df2

#Returns the most used hashtag in given time period
NOG12 = count_hashtags(df2012OG).show(50)
#Returns a filtered dataframe with an array of hashtags per tweet filtered on Olympic Game words
LOG12 = combine(unionAllv2([df2012OG]),game12,game12l)
#Returns the sport hashtags that are most tweeted about
OG12 = sports_count(LOG12,sportssl,sportensl).show(50)
#Returns the cultural hashtags that are moste tweeted about
COG12 = culture_count(combine(unionAllv2([df2012OG]),game12,game12l),culture2012).show(50)
# OG12.coalesce(1).write.format('json').save('/user/s1822306/OG12')

NOG14 = count_hashtags(df2014OG).show(50)
LOG14 = combine(unionAllv2([df2014OG]),game14,game14l)
OG14 = sports_count(LOG14,sportswl,sportenwl).show(50)
COG14 = culture_count(combine(unionAllv2([df2014OG]),game14,game14l),culture2014).show(50)
# OG14.coalesce(1).write.format('json').save('/user/s1822306/OG14')

NOG16 = count_hashtags(df2016OG).show(50)
LOG16 = combine(unionAllv2([df2016OG]), game16, game16l)
OG16 = sports_count(LOG16,sportssl,sportensl).show(50)
COG16 = culture_count(combine(unionAllv2([df2016OG]), game16, game16l), culture2016).show(50)
# OG16.coalesce(1).write.format('json').save('/user/s1822306/OG16')


OGs = unionAllv2([OG12,OG14,OG16])
OGs2 = OGs.sort(OGs[1].desc()).show(100)
# OGs2.coalesce(1).write.format('json').save('/user/s1822306/OG')

