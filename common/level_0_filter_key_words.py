import re
from re import Pattern
from retrie.retrie import Checklist

PUNCTUATION_REGEX = re.compile(r"""[?.,/\\><:;'"()!%$*|^~`+#]""")
SPACE_REGEX = re.compile(r"\s+")


def construct_distinct_word_regex(word_array) -> Pattern:
    # word_array = [w.lower() for w in word_array]
    word_regex_trie = Checklist(word_array, match_substrings=False, re_flags=re.IGNORECASE)
    return word_regex_trie.compiled


# relevant key words

harassment_key_words = [
    'armed disturb', 'bullied', 'bully',
    'coercion', 'communication threats', 'criminal threats',
    'cursed', 'cursing', 'cyberstalking',
    'defamation', 'ethnic intimidation', 'harassed',
    'harassing', 'harassing communications', 'harassmen',
    'harassment', 'harrassing', 'harrassment',
    'hate crime', 'hurled racist slurs', 'insult',
    'insulted', 'insulting', 'intimidate',
    'intimidated', 'intimidating', 'intimidation',
    'intimidation premise', 'intimidation with a dangerous weapon', 'menacing',
    'obscene', 'obscene phone call', 'obscene phone calls',
    'obscenity', 'obsenity exposing', 'peeping tom',
    'racial slurs', 'racially motivated attack', 'racist attack',
    'racist slur', 'racist slurs', 'sending threatening text and video messages',
    'stalking', 'telephone harassment',
    'threat weapon', 'threatening', 'threatened to kill', 'threatened to shoot',
    'abused',
    'threatening phone calls', 'threatening text', 'threatening to kill',
    'threatening to shoot', 'tracking',
    'unlawful exposure', 'verbal threats'
]

theft_key_words = [
    'alarm burglary', 'bar without payment', 'burglar',
    'burglaries', 'burglary', 'burglary alarm',
    'larceny', 'larnecy from motor vehicle', 'pocket-picking', 'larnecy shoplifting',
    'made off from a hotel', 'making off from a hotel', 'obtained a service without payment',
    'obtaining a service without payment', 'pickpocket', 'pickpocketing',
    'restaurant without payment', 'shop-lifting', 'shoplift',
    'shoplifted', 'shoplifting', 'steal',
    'stealing', 'stole', 'theft',
    'theft from motor vehicle', 'theft from person', 'theft investigation',
    'theft report'
]

robbery_key_words = [
    'armed robbery', 'break and enter', 'break-and-enter',
    'breaking entering forcemugging', 'holdup robbery', 'home invasion',
    'housebreaking', 'mugged', 'purse-snatching',
    'ransacked', 'ransacking', 'robbed',
    'robber', 'robberies', 'robbers',
    'robbery', 'robbery commercial', 'robbery premise',
    'robbing', 'robery armed', 'robery unarmed',
    'snatch', 'snatched', 'snatching',
    'violent robbery'
]

auto_theft_key_words = [
    'armed carjacking', 'auto burglary', 'auto theft',
    'bicycle larnecy', 'bicycle theft', 'burglary auto',
    'car jack', 'car theft', 'car was stolen',
    'car-theft', 'carjack', 'carjacked',
    'carjacker', 'carjackers', 'carjacking',
    'carjackings', 'commandeered the vehicle', 'grand larceny',
    'grand larceny bicycle', 'grand theft', 'grand theft auto',
    'gta', 'hijack', 'hijacked',
    'hijacking', 'hijackings', 'motor vehicle theft',
    'steal a car', 'steal car', 'stole a Chevy truck',
    'stole a car', 'stole a truck', 'stole a vehicle',
    'stole another motor vehicle', 'stole another vehicle', 'stole car',
    'stole her car', 'stole his car', 'stole his vehicle',
    'stole their car', 'stolen bicycle', 'stolen car',
    'stolen motor vehicle', 'stolen pickup truck', 'stolen van',
    'stolen vehicle', 'theft motor vehicle parts', 'theft of a motor vehicle',
    'theft vehicle', 'van robbers', 'vehicle breackinstolen vehicle',
    'vehicle break-in', 'vehicle burglary', 'vehicle crime',
    'vehicle grand', 'vehicle larceny', 'vehicle theft',
    'vehicle was burglarized', 'vehicle was stolen'
]

extortion_key_words = [
    'blackmail', 'extortio', 'extortion',
    'extortion blackmail', 'extortion blackmail premise', 'extortion investigation',
    'extortion threats'
]

kidnapping_key_words = [
    'abduct', 'abduct a newborn baby', 'abduction',
    'bound with duct tape', 'held hostage', 'kidnap',
    'kidnapped', 'kidnapping', 'kidnapping abduction',
    'kidnapping abduction premise', 'kidnapping neighbourhood', 'unlawful imprisonment',
    'unlawful restraint'
]

sex_offences_key_words = [
    'forcible fondling', 'forcible rape', 'forcible sodomy premise',
    'lewd conduct incident number', 'indecent exposure'
    'lewdness incident', 'masturbating in public',
    'molested', 'molesting', 'rape',
    'rape force', 'restraining order violation', 'sex abuse',
    'sex crime', 'sex crimes', 'sex crimes abuse',
    'sex offences', 'sex offender', 'sexoff',
    'sexual abuse', 'sexual assault', 'sexual battery',
    'sexual offender', 'sexually assaulted', 'sexually assaulting',
    'soliciting'
]

vandalism_key_words = [
    'arson', 'criminal damage', 'destruct property',
    'destruction property', 'graffiti', 'malicious mischief',
    'property damage', 'vandalism'
]

trafficking_illegal_goods = [
    'buy narcotics', 'drug deal', 'drug dealer',
    'drug dealers', 'drug dealing', 'drug problem',
    'drug violations', 'drugs dealer', 'drugs dealers',
    'drugs dealing', 'drugs problem', 'drugs violations',
    'import narcotics', 'narcotics manufacture', 'possess narcotics',
    'sell narcotics'
]

fraud_key_words = [
    'counterfeting', 'credit card abuse', 'credit card debit abuse',
    'credit card fraud', 'deceptive practice', 'delayed forgery',
    'embezzlement', 'false pretenses', 'false swindle',
    'false use anothers identity', 'fare evasion', 'financial identity theft',
    'forgery', 'fraud', 'fraud calls',
    'fraud credit card', 'fraud incident', 'identity theft',
    'impersonated'
]

organised_crime_key_words = [
    'assisting gambling', 'attemp conspiracy penalties', 'conspiracy commit crime',
    'criminal conspiracy', 'engaging organized criminal activity', 'gang',
    'gang activity', 'gang related', 'money laundering',
    'operating gambling', 'organized criminal activity', 'promoting gambling',
    'street gang'
]

terrorist_threats_key_words = [
    'bomb threat', 'terrorist threat', 'terrorist threats',
    'terroristic threat', 'terroristic threat zone', 'terroristic threatening',
    'terroristic threats', 'terrorizing'
]

disturbance_key_words = [
    'civil disturbance', 'disorderly conduct', 'disorderly person',
    'distubance family', 'disturb', 'disturbance',
    'disturbance business', 'disturbance neighbour', 'disturbing peace',
    'domestic disturbance', 'loud noise disturbance', 'noise complaint',
    'noise disturbance', 'noisy party', 'public disturbence',
    'public order'
]

suspicious_activity_key_words = [
    'suspicious activity', 'suspicious circumstances', 'suspicious event',
    'suspicious incident', 'suspicious perso', 'suspicious person',
    'suspicious priority', 'suspicious situation', 'suspicious subject',
    'suspicious vehicle'
]

domestic_offences_key_words = [
    'domestic assault', 'domestic battery', 'domestic dispute',
    'domestic incident', 'domestic progress', 'domestic related',
    'domestic trouble', 'domestic verbal', 'domestic violence',
    'family dispute', 'family fight', 'family trouble',
]

drug_alcohol_violations_key_words = [
    'alcohol violation', 'alcohol violations', 'disturbance firecrackers',
    'drug case', 'drug equipment violations', 'drug offence',
    'drug offences', 'drug overdose', 'drug paraphernalia',
    'drug violation', 'drugs',
    'drugs case', 'drugs narcotics', 'drugs offence',
    'drugs offences', 'drugs overdose',
    'narcotic', 'narcotics', 'narcotics offence',
    'narcotics offense', 'narcotics place', 'paraphernalia use',
    'possesion marijuana', 'possess controlled substance'
]

traffic_violations_key_words = [
    'accident hit run', 'drag racing', 'driving impaired',
    'driving influence', 'driving influence premise', 'driving intoxicated',
    'driving under the influence of alcohol', 'driving violation', 'drunk drive',
    'drunk driver', 'drunk driving', 'dui',
    'dui alcohol', 'hit and run', 'hit run', 'hit run collision',
    'hit run property', 'hit-and-run',
    'impaired driver', 'impaired driving', 'intoxicated driver',
    'motor vehicle crash accident', 'motor vehicle violation', 'parking violation',
    'ran a red light', 'ran red light', 'reckless driver',
    'reckless driving', 'run a red light', 'run red light',
    'street race', 'street races', 'traffic offense',
    'traffic-moving violations'
]

trespassing_key_words = [
    'trespassing', 'trespass', 'trespasser',
    'alarm intrusion incident', 'intrusion'
]

weapon_violations_key_words = [
    'armed person', 'brandishing weapon', 'discharging firearm',
    'firearm\'s serial number was obliterated', 'illegally possessed assault rifle', 'illegally possessed gun',
    'illegally possessed rifle', 'possession weapons', 'serial number was obliterated',
    'shots fire', 'shots fired',
    'shots heard', 'shotspotter', 'sound gunshots',
    'weapon', 'weapon law violations', 'weapons',
]

assault_key_words = [
    'aggravated assault', 'assault', 'assaulted',
    'assaulting', 'attempt to murder', 'attempted homicide',
    'attempted murder', 'attempts or threats to murder', 'beaten',
    'beaten to unconsciousness', 'beating a man', 'beating man',
    'beating men', 'beating the man', 'bodily harm',
    'brutally beat', 'common assault', 'grievous bodily harm',
    'hit and kick', 'hurted', 'injured',
    'injury to', 'knife wounds', 'knifing',
    'serious bodily injury', 'serious injury', 'simple assault',
    'simply bodily harm', 'stab', 'stab wound',
    'stabbed', 'stabbing', 'struck',
    'violently attacked'
]

homicide_key_words = [
    'assassination', 'concealment of a body', 'deadly shootingslaying',
    'death investigation', 'deceased victims', 'declared dead',
    'did not survive the shooting', 'died after', 'died after shots fired',
    'died of a single gunshot wound', 'died of multiple gunshot wounds', 'dismembered',
    'fatal head injury', 'fatal stabbing', 'fatal stabbings',
    'fatally shot', 'fatally stabed', 'genocide',
    'homicide', 'kill', 'killed',
    'lynching', 'manslaughter', 'massacre',
    'murder', 'mutilating a corpse', 'person is dead',
    'reported dead by gunshot wound', 'shooting', 'shooting death',
    'shot', 'shot dead', 'shots',
    'stabbed to death', 'suffocated', 'was found deadinvestigated as homicides',
    'was shot to death', 'were found dead', 'were shot to death',
]

# not-relevant key words

trials_key_words = [
    'Court jury', 'Court jury found', 'District Court',
    'Presidential Records Act', 'Supreme Court',
    'Supreme Court of the United States', 'U.S. District Court', 'U.S. Supreme Court decision',
    'US Department of Justice', 'acting on a request from prosecutors', 'allegation',
    'allegations', 'announced indictments', 'awaits extradition to',
    'according to an affidavit', 'affidavit states',
    'being held on a fugitive', 'from justice warrant',
    'ustice warrant', 'warrants for', 
    'closely supervised probation', 'convicted', 'court imposed curfew',
    'court-imposed curfew', 'charges upgraded', 'charge upgraded'
    'death by lethal injection', 'deferred sentence for',
    'definition of justified homicide', 'denied parole', 'defendants', 'defendant',
    'ethics complaint', 'investigation is underway',
    'execution of a death', 'extradicted', 'federal judge',
    'filed an ethics complaint against', 'federal arrest warrant was issued',
    'guilty pleas', 'has been denied parole',
    'has been extradited', 'has been found guilty', 'have been found guilty',
    'introduced a resolution', 'judge', 'judge has denied',
    'judge prohibited', 'judge\'s sentence', 'judges',
    'judges have denied', 'juries', 'jury',
    'jury ruled in favour of', 'juvenile petition', 'juvenile petitions',
    'lawsuit', 'lawsuits', 'life sentence',
    'mistrial', 'next hearing is set for', 'no unsupervised contact with',
    'not guilty pleas', 'opportunity for parole', 'paroled',
    'petition filed for voluntary', 'pleaded guilty', 'pleaded guilty before U.S. District Court',
    'pleading guilty', 'probation', 'probations',
    'prosecutor', 'prosecutors', 'schedule to be killed',
    'scheduled to be killed', 'scheduled to be put to death', 'sentenced',
    'sentenced to life in prison', 'settle', 'settlement',
    'special grand jury', 'sued', 'suspended sentence for',
    'subpoenas', 'subpoena',
    'testified', 'the court entered not guilty pleas', 'trial',
    'trial began in', 'trials', 'verdict',
    'was dismissed against', 'was extradicted', 'was extradited back',
    'was justified', 'were extradicted', 'were extradicted back',
    'were justified', 'with conditions of release', 'wanted in connection to',
]

car_accidents_key_words = [
    'traffic crash', 'car accident',
    'car crashed', 'truck crashed',
    'crash', 'crashed'
]

statistics_key_words = [
    '10-year average', 'Anti-Defamation League', 'Motor vehicle theft is up',
    'Police Department homicide', 'alarming rate of carjackings',
    'annual death rates', 'arrests each year', 'based on provisional data',
    'rise in gun related crimes', 'crime index rate', 'crime rate per',
    'crime trends', 'homicide unit', 'lower crime index rate',
    'lower property crime index', 'number of crimes per', 'number of crimes per 1,000 people',
    'number of crimes per 1000 people', 'one-in-10', 'one-in-10 female',
    'million kids aged',
    'overall theft is up', 'police robbery/homicide detectives',
    'property crime index', 'provisional data', 'quarterly',
    'record number', 'spikes in', 'spikes in shootings',
    'statewide', 'theft prevention unit', 'violent index crimes',
    'incidents in the 24-hour period', 'OVI-related crashes last year', 
]

gun_policy_key_words = [
    'FOID card applications', 'Uvalde highlights', 'Uvalde shooting',
    'Uvalde slaughter', 'anti gun zealots', 'anti-gun groups',
    'anti-gun zealots', 'assault weapon ban', 'assault weapons ban',
    'concealed carry of', 'concealed carry permit', 'gun bill',
    'gun control', 'gun control advocates', 'gun control laws',
    'gun haters', 'gun laws', 'gun measures',
    'gun owners', 'gun ownership', 'gun policy',
    'gun policy measures', 'gun related restrictions', 'gun restrictions',
    'gun sales went through the roof', 'gun-free zones', 'gun-related restrictions',
    'law for concealed carry', 'million guns sold', 'regarding FOID card',
    'relationship between state gun ownership rates', 'requirements for carrying a handgun', 'restrict guns',
    'semi-automatic weapon ban', 'signed a new gun bill', 'stockpiling weapons',
    'suspicious weapons sales', 'tracking gun sales', 'tracking large-scale firearms purchases',
]

abortion_key_words = [
    'abortion advocates', 'abortion ban', 'abortion decision',
    'abortion disclosure form', 'abortion disclosure forms', 'abortion law',
    'exceptions for instances of rape', 'pro-abortion', 'pro-abortion group',
    'pro-abortion propaganda', 'pro-life activist', 'pro-life laws',
    'provided an abortion', 'state records provided an abortion',
    'state\'s new abortion law', 'state new abortion law', 'state abortion law'
]

politics_key_words = [
    'Amendment', 'Biden administration', 'Democrat opponent',
    'Democratic Senator', 'Donald Trump', 'GDP', 'Gov.',
    'President Donald Trump', 'President Joe Biden', 'Republican Party',
    'Republican Senate candidate', 'US senators are asking President', 'constitution specifies',
    'constitution was adopted', 'constitutional convention', 'gross domestic product',
    'Lawmakers in Congress',
    'law aimed at', 'cracking down on violent protests', 'legislation',
    'legislation would limit', 'legislature', 'municipal elections',
    'political weapon', 'property crimes bill', 'signed House Bill',
    'signed a bill into law', 'signed the bill into law', 'sponsored in the state Senate',
    'steps to stabilise economy', 'stronger penalties', 'stronger penalty',
    'this year\'s election', 'threats to public health', 'voting laws',
    'voting rights restored', 'special election', 'vacant city council seats',
    'election was done by hand', 'Christian nationalism',
    'racist ideology', 'anti-transgender healthcare protesters',
    'anti-transgender protesters',
]

blaze_key_words = [
    'Firefighters', 'blaze', 'brush fire',
    'brush fire off', 'destructive wildfire', 'fire broke out',
    'firefighters battled the fire', 'firefighters stop the spread', 'mopping up a burn area',
    'wildfire', 'wildland fire'
]

film_key_words = [
    'DC Comics', 'Film Festival', 'Focused completely on real events',
    'Jeffrey Dahmer', 'Netflix', 'Netflix\'s',
    'Vogue article', 'actor was shooting', 'album of songs',
    'anime series', 'at the premiere', 'based on real events',
    'debut film', 'horror film', 'docuseries',
    'film revolves around', 'first premiered in', 'hit the full interview above',
	'live shot', 'rehearsal',
    'scene had to be shot with', 'science-fiction', 'shooting for film',
    'shooting for her Hollywood debut film', 'star as', 'star in film',
    'star in the film', 'starred in film', 'starred in the film',
    'starring', 'stole the show', 'stole the show at the premiere',
    'streaming service', 'tops chart', 'tops the Netflix chart',
    'tops the chart', 'trailers for'
]

weather_key_words = [
    'Mostly clear and cool', 'Mostly clear', 'clear and cool', 'Mostly sunny',
    'increasing clouds', 'possibility of a tornado',
    'severe weather', 'severe winds', 'storm survey',
    'storm tracking', 'tornado warning', 'tornado warnings',
    'tornado warnings expired', 'tracking a chance of storms', 'tracking closures',
    'tracking low pressure to our east', 'weekend starts off warm'
]

covid_key_words = [
    'COVID-19 booster shots', 'COVID-19 shots', 'COVID-19 struck',
    'COVID-19 vaccines for children', 'Covid vaccines', 'Pfizer shot',
    'Pfizer shots', 'eligible for the shots', 'offering shots to children',
    'shots for those under five', 'shots for under five', 'shots from Moderna and Pfizer',
    'tracking Covid-19', 'vaccine is safe'
]

other_key_words = [
    '9/11 attacks', '9/11 videos', 'All fireworks are illegal',
    'Coalition Against Sexual Assault', 'Department of Transportation', 'Ground Zero',
    'PETA supporters', 'September 11th, 2001', 'Today we solemnly remember the lives',
    'active shooter training session', 'army closed areas', 'attacked by a large shark',
    'carrying his squad to victory', 'coins struck', 'cut pharmacy robberies',
    'fight for a livable wage', 'fireworks use', 'flag known patterns of suspicious activity',
    'the upper bracket final', 'guide for combating bullying', 'hit match point',
    'honors a fallen officer', 'honors officer', 'injured in a shark attack',
    'intrusion plates', 'irrigation efforts', 'month investigation of the case',
    'mourned the victims', 'non-emergency lockdown', 'radio tracking collar',
    'right to form a union', 'safer working conditions', 'steel intrusion plates',
    'struck a panic', 'struck panic', 'time delay safes',
    'turned to drugs', 'twin towers in New York City', 'upper bracket final',
    '9/11 terrorist attacks', 'winning a round',
    'found dead in his cell', 'years since the attack'
]

# list of relevant key words
relevant_key_words_list = [
    harassment_key_words, theft_key_words,
    robbery_key_words, auto_theft_key_words,
    assault_key_words, extortion_key_words,
    kidnapping_key_words, sex_offences_key_words,
    vandalism_key_words, trafficking_illegal_goods,
    fraud_key_words, organised_crime_key_words,
    homicide_key_words, terrorist_threats_key_words,
    disturbance_key_words, suspicious_activity_key_words,
    domestic_offences_key_words, drug_alcohol_violations_key_words,
    traffic_violations_key_words, trespassing_key_words,
    weapon_violations_key_words
]

# list of not relevant key words
not_relevant_key_words_list = [
    trials_key_words, car_accidents_key_words,
    statistics_key_words, gun_policy_key_words,
    abortion_key_words, politics_key_words,
    blaze_key_words, film_key_words,
    weather_key_words, covid_key_words,
    other_key_words
]

all_key_words_list = [
    *relevant_key_words_list,
    *not_relevant_key_words_list
]

all_key_words_list = [
    *relevant_key_words_list, *not_relevant_key_words_list
]

# relevant regex with key words
HARASSMENT_KEY_WORDS_REGEX = construct_distinct_word_regex(harassment_key_words)
THEFT_KEY_WORDS_REGEX = construct_distinct_word_regex(theft_key_words)
ROBBERY_KEY_WORDS_REGEX = construct_distinct_word_regex(robbery_key_words)
AUTO_THEFT_KEY_WORDS_REGEX = construct_distinct_word_regex(auto_theft_key_words)
ASSAULT_KEY_WORDS_REGEX = construct_distinct_word_regex(assault_key_words)
EXTORTION_KEY_WORDS_REGEX = construct_distinct_word_regex(extortion_key_words)
KIDNAPING_KEY_WORD_REGEX = construct_distinct_word_regex(kidnapping_key_words)
SEX_OFFENCES_KEY_WORD_REGEX = construct_distinct_word_regex(sex_offences_key_words)
VANDALISM_KEY_WORDS_REGEX = construct_distinct_word_regex(vandalism_key_words)
TRAFFICKING_ILLEGAL_GOODS_REGEX = construct_distinct_word_regex(trafficking_illegal_goods)
FRAUD_KEY_WORDS_REGEX = construct_distinct_word_regex(fraud_key_words)
ORGANISED_CRIME_KEY_WORDS_REGEX = construct_distinct_word_regex(organised_crime_key_words)
HOMICIDE_KEY_WORDS_REGEX = construct_distinct_word_regex(homicide_key_words)
TERRORIST_THREATS_KEY_WORDS_REGEX = construct_distinct_word_regex(terrorist_threats_key_words)
DISTURBANCE_KEY_WORDS_REGEX = construct_distinct_word_regex(disturbance_key_words)
SUSPICIOUS_ACTIVITY_KEY_WORDS_REGEX = construct_distinct_word_regex(suspicious_activity_key_words)
DOMESTIC_OFFENCES_KEY_WORDS_REGEX = construct_distinct_word_regex(domestic_offences_key_words)
DRUG_ALCOHOL_KEY_WORDS_REGEX = construct_distinct_word_regex(drug_alcohol_violations_key_words)
TRAFFIC_VIOLATIONS_KEY_WORDS_REGEX = construct_distinct_word_regex(traffic_violations_key_words)
TRESPASSING_KEY_WORDS_REGEX = construct_distinct_word_regex(trespassing_key_words)
WEAPON_VIOLATIONS_KEY_WORDS_REGEX = construct_distinct_word_regex(weapon_violations_key_words)

# not relevant regex with key words
TRIAL_KEY_WORDS_REGEX = construct_distinct_word_regex(trials_key_words)
CAR_ACCIDENT_KEY_WORDS_REGEX = construct_distinct_word_regex(car_accidents_key_words)
STATS_KEY_WORDS_REGEX = construct_distinct_word_regex(statistics_key_words)
GUN_POLICY_KEY_WORDS_REGEX = construct_distinct_word_regex(gun_policy_key_words)
ABORTION_KEY_WORDS_REGEX = construct_distinct_word_regex(abortion_key_words)
POLITICS_KEY_WORDS_REGEX = construct_distinct_word_regex(politics_key_words)
BLAZE_KEY_WORDS_REGEX = construct_distinct_word_regex(blaze_key_words)
FILM_KEY_WORDS_REGEX = construct_distinct_word_regex(film_key_words)
WEATHER_KEY_WORDS_REGEX = construct_distinct_word_regex(weather_key_words)
COVID_KEY_WORDS_REGEX = construct_distinct_word_regex(covid_key_words)
OTHER_KEY_WORDS_REGEX = construct_distinct_word_regex(other_key_words)

# list of relevent regex
relevant_regex_list = [
    HARASSMENT_KEY_WORDS_REGEX, THEFT_KEY_WORDS_REGEX,
    ROBBERY_KEY_WORDS_REGEX, AUTO_THEFT_KEY_WORDS_REGEX,
    ASSAULT_KEY_WORDS_REGEX, EXTORTION_KEY_WORDS_REGEX,
    KIDNAPING_KEY_WORD_REGEX, SEX_OFFENCES_KEY_WORD_REGEX,
    VANDALISM_KEY_WORDS_REGEX, TRAFFICKING_ILLEGAL_GOODS_REGEX,
    FRAUD_KEY_WORDS_REGEX, ORGANISED_CRIME_KEY_WORDS_REGEX,
    HOMICIDE_KEY_WORDS_REGEX, TERRORIST_THREATS_KEY_WORDS_REGEX,
    DISTURBANCE_KEY_WORDS_REGEX, SUSPICIOUS_ACTIVITY_KEY_WORDS_REGEX,
    DOMESTIC_OFFENCES_KEY_WORDS_REGEX, DRUG_ALCOHOL_KEY_WORDS_REGEX,
    TRAFFIC_VIOLATIONS_KEY_WORDS_REGEX, TRESPASSING_KEY_WORDS_REGEX,
    WEAPON_VIOLATIONS_KEY_WORDS_REGEX
]

# list of not relevant regex
not_relevant_regex_list = [
    TRIAL_KEY_WORDS_REGEX,
    CAR_ACCIDENT_KEY_WORDS_REGEX,
    STATS_KEY_WORDS_REGEX,
    GUN_POLICY_KEY_WORDS_REGEX,
    ABORTION_KEY_WORDS_REGEX,
    POLITICS_KEY_WORDS_REGEX,
    BLAZE_KEY_WORDS_REGEX,
    FILM_KEY_WORDS_REGEX,
    WEATHER_KEY_WORDS_REGEX,
    COVID_KEY_WORDS_REGEX,
    OTHER_KEY_WORDS_REGEX
]

# list of names relevant categories
relevant_categories_names = [
    'harassment', 'theft',
    'robbery', 'auto_theft',
    'assault', 'exortion',
    'kidnapping', 'sex_offences',
    'vandalism', 'trafficking_illegalgoods',
    'fraud', 'organised_crime',
    'homicide', 'terrorist_threats',
    'diturbance', 'suspicious_activity',
    'domestic_offences', 'drugalcohol_violations',
    'traffic_violations', 'trespassing',
    'weapon_violations'
]


# list of names not relevant categories
not_relevant_categories_names = [
    'trial',
    'car_accident',
    'statistics',
    'gun_policy',
    'abortion',
    'politics',
    'blaze',
    'film',
    'weather',
    'covid',
    'other',
]