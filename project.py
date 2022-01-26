#Names:
#Jelle Maas s1822306
#Allesandra Amato s2886561
#Rosalie Voorend s2134861
#Valeria Pinus s2844559

import sys
reload(sys)
sys.setdefaultencoding('utf-8')



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, array_contains, lower
spark = SparkSession.builder.getOrCreate()

def lower_array(array):
	lowercase_array = []
	for i in range(len(array)):
		lowercase_array.append(array[i].lower())
	return lowercase_array

always = ["teamNL", "OS", "Olympics", "GoTeamNL", "OlympicTeamNL", "Goud", "Brons", "Zilver", "Mannen", "Dames", "Finale", "NL", "Oranje", "Openingceremony", "Trots", "Heineken"]
alwaysl = lower_array(always)
game12 = ["OS2012", "Zomerspelen", "Londen", "London2012", "Londen2012", "OlympischeSpelen2012", "EpkeZonderland", "O2arena", "Londen", "Uk", "UK", "NOS"]
game12l = lower_array(game12)
game14 = ["OlympischeSpelen2014", "Winterspelen" "Sochi", "Sotchi2014", "Sochi2014", "Sotsji2014", "OS2014", "Sotsji", "Sotchi", "Sochi", "Kramer", "Bergsma", "Rusland", "OlympischeWinterSpelen2014", "1000M"]
game14l = lower_array(game14)
game16 = ["OlympischeSpelen2016", "Rio2016", "Zomerspelen", "Rio", "Brazil", "Brugge", "RioDeJaneiro", "OS2016"]
game16l = lower_array(game16)
game18 = ["Schulting", "Winterspelen", "Pyeongchang", "OlympischeSpelen2018", "Pyeongchang2018", "Nuis", "Wereldrecord", "Shorttrack", "Schaatsen", "10KM", "OS2018", "Collectievissen"]
game18l = lower_array(game18)
sportss = ["Archery", "Boxing", "Canoe", "Kayak", "Cycling", "Diving", "Equestrian", "Fencing", "Field Hockey", "Gymnastics", "Modern Pentathlon", "Pentathlon", "Rowing", "Sailing", "Shooting", "Soccer", "Swimming", "Synchronized Swimming", "Table Tennis", "Track and Field", "Triathlon", "Volleyball", "Water Polo", "Weightlifting", "Wrestling"]
sportssl = lower_array(sportss)
sportens = ["Boogschieten", "Badminton", "Boksen", "Kano", "Kajak", "Fietsen", "Wielrennen", "Duiken", "Paardrijden", "Schermen", "Hockey", "Gymnastiek","Turnen", "Judo", "Moderne vijfkamp", "vijfkamp", "roeien", "zeilen", "schieten", "voetbal", "zwemmen", "gesynchroniseerd zwemmen", "tafeltennis","pingpong", "taekwondo", "tennis", "Triatlon", "Volleybal","Beach", "Waterpolo", "Gewichtheffen", "Worstelen"]
sportensl = lower_array(sportens)
sportsw = ["Alpine Skiing", "Biathlon", "Bobsled", "Cross Country Skiing", "Curling", "Figure Skating", "Freestyle Skiing", "Ice Hockey", "Luge", "Nordic Combined", "Skeleton", "Ski Jumping", "Snowboarding", "Speedskating", "Ice Skating", "Skiing"]
sportswl = lower_array(sportsw)
sportenw = ["Alpineskien", "Biatlon", "Bobslee", "Langlaufen", "Curlen", "Kunstschaatsen", "Freestyle Skien", "IJshockey", "Rodelen", "Noorse combinatie", "Skispringen", "Snowboarden", "Snelschaatsen", "Schaatsen", "Skien"]
sportenwl = lower_array(sportenw)

culture2012 = ["jamesbond", "queen", "elizabeth", "queenelizabeth", "mrbean", "bean", "rowan", "atkinson", "rowanatkinson", "daniel", "craig", "danielcraig", "arcticmonkey", "spicegirls", "edsheeran", "1d", "onedirection","directioner", "harry", "liam", "niall", "louis", "zayn"]
culture2014 = ["tolstoy", "putin", "zwanenmeer", "swanlake", "maria", "sharapova", "mariasharapova"]
culture2016 = ["kygo", "giselebundchen", "gisele", "bundchen", "forrest", "carnaval"]


def unionAllv2(dfs, total=None):
	i = 0
	length = len(dfs)
	been_here = False
	if length > 1:
		while i < length:
			if i+1 < length and been_here != True:
				total = dfs[i+1].union(dfs[i])
				i +=2
				been_here = True
			else:
				total = total.union(dfs[i])
				i +=1
		return total
	else:
		return dfs[0]

def hashtag_retrieve(df):
	filtered_df = df.select(col("entities")["hashtags"]["text"].alias("hashtags")).filter(size(col("hashtags")) > 0)
	return filtered_df

def hashtag_user_retrieve(df):
	filtered_df = df.select(col("entities")["hashtags"]["text"].alias("hashtags"),col("user")["name"].alias("User") ).filter(size(col("hashtags")) > 0)
	return filtered_df

#Winter Olympics: 7-23 februari 2014 (Sochi),
# /data/twitterNL/201402/201402**-**.out.gz     complete february 2014


# /data/twitterNL/201402/20140207-**.out.gz     7 feb
# /data/twitterNL/201402/20140208-**.out.gz     8 feb
# /data/twitterNL/201402/20140209-**.out.gz     9 feb
# /data/twitterNL/201402/2014021*-**.out.gz     10-19 feb
# /data/twitterNL/201402/20140220-**.out.gz     20 feb
# /data/twitterNL/201402/20140221-**.out.gz     21 feb
# /data/twitterNL/201402/20140222-**.out.gz     22 feb
# /data/twitterNL/201402/20140223-**.out.gz     23 feb



# dfs2 = spark.read.json("/data/twitterNL/201402/20140207-**.out.gz")
# dfs3 = spark.read.json("/data/twitterNL/201402/20140208-**.out.gz")
# dfs4 = spark.read.json("/data/twitterNL/201402/20140209-**.out.gz")
# dfs5 = spark.read.json("/data/twitterNL/201402/2014021*-**.out.gz")
# dfs6 = spark.read.json("/data/twitterNL/201402/20140220-**.out.gz")
# dfs7 = spark.read.json("/data/twitterNL/201402/20140221-**.out.gz")
# dfs8 = spark.read.json("/data/twitterNL/201402/20140222-**.out.gz")
# dfs9 = spark.read.json("/data/twitterNL/201402/20140223-**.out.gz")
#
#
# df2014OG = unionAllv2([hashtag_retrieve(dfs2),hashtag_retrieve(dfs3),hashtag_retrieve(dfs4),hashtag_retrieve(dfs5),hashtag_retrieve(dfs6),hashtag_retrieve(dfs7),hashtag_retrieve(dfs8),hashtag_retrieve(dfs9)])

#Summer Olympics: 5-21 augustus 2016
# /data/twitterNL/201608/201608**-**.out.gz     complete august 2016


# /data/twitterNL/201608/20160805-**.out.gz     5 aug
# /data/twitterNL/201608/20160806-**.out.gz     6 aug
# /data/twitterNL/201608/20160807-**.out.gz     7 aug
# /data/twitterNL/201608/20160808-**.out.gz     8 aug
# /data/twitterNL/201608/20160809-**.out.gz     9 aug
# /data/twitterNL/201608/2016081*-**.out.gz     10-19 aug
# /data/twitterNL/201608/20160820-**.out.gz     20 aug
# /data/twitterNL/201608/20160821-**.out.gz     21 aug


dfr1 = spark.read.json("/data/twitterNL/201608/20160805-**.out.gz")
dfr2 = spark.read.json("/data/twitterNL/201608/20160806-**.out.gz")
dfr3 = spark.read.json("/data/twitterNL/201608/20160807-**.out.gz")
dfr4 = spark.read.json("/data/twitterNL/201608/20160808-**.out.gz")
dfr5 = spark.read.json("/data/twitterNL/201608/20160809-**.out.gz")
dfr6 = spark.read.json("/data/twitterNL/201608/2016081*-**.out.gz")
dfr7 = spark.read.json("/data/twitterNL/201608/20160820-**.out.gz")
dfr8 = spark.read.json("/data/twitterNL/201608/20160821-**.out.gz")


df2016OG = unionAllv2([hashtag_retrieve(dfr1),hashtag_retrieve(dfr2),hashtag_retrieve(dfr3),hashtag_retrieve(dfr4),hashtag_retrieve(dfr5),hashtag_retrieve(dfr6),hashtag_retrieve(dfr7),hashtag_retrieve(dfr8)])


#Summer Olympics: 27-12 augustus 2012
# /data/twitterNL/201207/2012072*-**.out.gz     20 july 2012 to 30 july 2012
# /data/twitterNL/201207/2012073*-**.out.gz     30 july to 31 july 2012
# /data/twitterNL/201208/201208**-**.out.gz     complete august 2012

# /data/twitterNL/201207/20120727-**.out.gz     27 july
# /data/twitterNL/201207/20120728-**.out.gz     28 july
# /data/twitterNL/201207/20120729-**.out.gz     29 july
# /data/twitterNL/201207/2012073*-**.out.gz     30 july to 31 july 2012
# /data/twitterNL/201208/2012080*-**.out.gz     1-9 august
# /data/twitterNL/201208/20120810-**.out.gz     10 august
# /data/twitterNL/201208/20120811-**.out.gz     11 august
# /data/twitterNL/201208/20120812-**.out.gz     12 august

# dfl1 = spark.read.json("/data/twitterNL/201207/20120727-**.out.gz")
# dfl2 = spark.read.json("/data/twitterNL/201207/20120728-**.out.gz")
# dfl3 = spark.read.json("/data/twitterNL/201207/20120729-**.out.gz")
# dfl4 = spark.read.json("/data/twitterNL/201207/2012073*-**.out.gz")
# dfl5 = spark.read.json("/data/twitterNL/201208/2012080*-**.out.gz")
# dfl6 = spark.read.json("/data/twitterNL/201208/20120810-**.out.gz")
# dfl7 = spark.read.json("/data/twitterNL/201208/20120811-**.out.gz")
# dfl8 = spark.read.json("/data/twitterNL/201208/20120812-**.out.gz")
#
#
# df2012OG = unionAllv2([hashtag_retrieve(dfl1),hashtag_retrieve(dfl2),hashtag_retrieve(dfl3),hashtag_retrieve(dfl4),hashtag_retrieve(dfl5),hashtag_retrieve(dfl6),hashtag_retrieve(dfl7),hashtag_retrieve(dfl8)])






def count_hashtags(dff):
	df = dff.select(explode(col("hashtags")))
	df2 = df.groupBy(lower(df[0]).alias("hashtags")).count()
	df3 = df2.sort(df2[1].desc())
	return df3

def combine(df,spht,sphtl):
	#To-Do: array_contains with lower case words, gives error because it is an array
	df_array = []
	df1 = []
	df2 = []
	df3 = []
	df4 = []
	for word in always:
		df_array.append(df.select(col("hashtags")).filter(array_contains(col("hashtags"),word)))
		df1 = unionAllv2(df_array)
		df_array = []
	for word in alwaysl:
		df_array.append(df.select(col("hashtags")).filter(array_contains(col("hashtags"),word)))
		df2 = unionAllv2(df_array)
		df_array = []
	for word in spht:
		df_array.append(df.select(col("hashtags")).filter(array_contains(col("hashtags"),word)))
		df3 = unionAllv2(df_array)
		df_array = []
	for word in sphtl:
		df_array.append(df.select(col("hashtags")).filter(array_contains(col("hashtags"),word)))
		df4 = unionAllv2(df_array)
		df_array = []
	return count_hashtags(unionAllv2([df1,df2,df3,df4]))

def sports_count(df,sportsengl,sportsdutchl):
	dfs = []
	df1 = []
	df2 = []
	for word in sportsengl:
		dfs.append(df.select(col("hashtags"),col("count")).filter(df.hashtags == word))
		df1 = unionAllv2(dfs)
		dfs = []
	for word in sportsdutchl:
		dfs.append(df.select(col("hashtags"),col("count")).filter(df.hashtags == word))
		df2 = unionAllv2(dfs)
	df = unionAllv2([df1,df2])
	df2 = df.sort(df[1].desc())
	return df2

def culture_count(df,culturel):
	dfs = []
	for word in culturel:
		dfs.append(df.select(col("hashtags"),col("count")).filter(df.hashtags == word))
	df1 = unionAllv2(dfs)
	df2 = df1.sort(df1[1].desc())
	return df2

# dff1 = spark.read.json("/data/twitterNL/201402/20140207-22.out.gz")
# dff2 = spark.read.json("/data/twitterNL/201402/20140207-23.out.gz")

#NOG12 = count_hashtags(df2012OG).show(50)
# LOG12 = combine(unionAllv2([df2012OG]),game12,game12l)
# OG12 = sports_count(LOG12,sportssl,sportensl).show(50)
#COG12 = culture_count(combine(unionAllv2([df2012OG]),game12,game12l),culture2012).show(50)
#OG12.coalesce(1).write.format('json').save('/user/s182230/OG12')

#NOG14 = count_hashtags(df2014OG).show(50)
# LOG14 = combine(unionAllv2([df2014OG]),game14,game14l)
# OG14 = sports_count(LOG14,sportswl,sportenwl).show(50)
#COG14 = culture_count(combine(unionAllv2([df2014OG]),game14,game14l),culture2014).show(50)
#OG14.coalesce(1).write.format('json').save('/user/s182230/OG14')

#NOG16 = count_hashtags(df2016OG).show(50)
LOG16 = combine(unionAllv2([df2016OG]),game16,game16l)
#OG16 = sports_count(LOG16,sportssl,sportensl).show(50)
COG16 = culture_count(combine(unionAllv2([df2016OG]),game16,game16l),culture2016).show(50)
#OG16.coalesce(1).write.format('json').save('/user/s182230/OG16')


# OGs = unionAllv2([OG12,OG14,OG16])
# OGs2 = OGs.sort(OGs[1].desc()).show(100)
#OGS2.coalesce(1).write.format('json').save('/user/s2844559/OG')
