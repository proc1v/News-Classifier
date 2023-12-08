# This is mapper from crime categories in ES to EN
map_event_types_from_es_to_en = {
    "Acoso": "Harassment",
    "Robo": "Robbery",
    "Robo_de_Coche": "Auto Theft",
    "Asalto": "Assault",
    "Extorcion": "Extortion",
    "Secuestro": "Kidnapping",
    "Violacion": "Sexual Violence",
    "Vandalismo": "Vandalism",
    "Pirateria": "Piracy",
    "Trafico_de_Materias_Ilegales": "Trafficking of Illegal Goods",
    "Fraude": "Fraud",
    "Actividad_de_Crimen_Organizado": "Organized Crime Activity",
    "Homicidio": "Homicide",
    "Otro": "Other",
    "Terrorismo": "Terrorism",
    "Corrupción": "Corruption",
    "Violent_Crimes": "Violent_Crimes",
    "Hurto": "Theft",
    "Actividad_Sospechosa": "Suspicious_Activity",
    "Desorden": "Disturbance",
    "Violaciones_de_Transito": "Traffic_Violation",
    "Ofensas_Domesticas": "Domestic_Offences"
}


def crimemapper(input_crime):
    raw_crime = str(input_crime).lower().strip()
    crime = "Otro"

    if (
            (
                    "piracy" in raw_crime or "pirateria" in raw_crime or "imitation" in raw_crime
            )
            and "total" not in raw_crime
    ):
        crime = "Pirateria"

    if (
            (
                    "obscenity" in raw_crime
                    or "以滋扰他" in raw_crime
                    or "侮辱" in raw_crime
                    or "威胁" in raw_crime
                    or "寻衅滋事" in raw_crime
                    or "猥亵" in raw_crime
                    or "intimidacion" in raw_crime
                    or "intimidación" in raw_crime
                    or "difamacion" in raw_crime
                    or "discriminacion" in raw_crime
                    or "tracking" in raw_crime
                    or "amenaza" in raw_crime
                    or "harrassment" in raw_crime
                    or "harassment" in raw_crime
                    or "harassing" in raw_crime
                    or "stalking" in raw_crime
                    or "bully" in raw_crime
                    or "threat" in raw_crime
                    or "acoso" in raw_crime
                    or "intimidaci" in raw_crime
                    or "intimidation" in raw_crime
                    or "peeping tom" in raw_crime
                    or "menac" in raw_crime
                    or "ultraje" in raw_crime
                    or "amenazas" in raw_crime
                    or "minacce" in raw_crime
                    or "ingiurie" in raw_crime
                    or "脅迫" in raw_crime
                    or "恐喝" in raw_crime
                    or "insult" in raw_crime
                    or "cruelty" in raw_crime
                    or "intent to outrage her modesty" in raw_crime
                    or "harass" in raw_crime
                    or "disorderly" in raw_crime
                    or "indecent" in raw_crime
                    or "indecency" in raw_crime
                    or "unlawful exposure" in raw_crime
                    or "annoy" in raw_crime
                    or "accosting" in raw_crime
                    or "intimidating" in raw_crime
                    or ("obscene" in raw_crime and "phone call" in raw_crime)
                    or "dis-conduct" in raw_crime
                    or "amenaces" in raw_crime
                    or "vexacions" in raw_crime
                    or "injúr" in raw_crime
                    or "骚扰" in raw_crime
                    or "garázdaságok" in raw_crime
                    or "zalezovanje" in raw_crime
                    or "grožnja" in raw_crime
                    or "armed disturb" in raw_crime
            )
            and "total" not in raw_crime
            and "domest" not in raw_crime
            and "animal" not in raw_crime
    ):
        crime = "Acoso"

    if (
            (
                    "theft" in raw_crime
                    or "盗窃" in raw_crime
                    or "vols" in raw_crime
                    or "industrial espionage" in raw_crime
                    or "vol" in raw_crime
                    or "auto theft" in raw_crime
                    or "b&e" in raw_crime
                    or "vehicle crime" in raw_crime
                    or "burgl" in raw_crime
                    or "burglar" in raw_crime
                    or "burglary" in raw_crime
                    or "burg" in raw_crime
                    or "b&" in raw_crime
                    or "stole" in raw_crime
                    or "shoplifting" in raw_crime
                    or "larceny" in raw_crime
                    or "larc" in raw_crime
                    or "gta" in raw_crime
                    or "snatch" in raw_crime
                    or "lojack"
                    in raw_crime  # for lojack activation (probably breaking into or stealing car)
                    or "snatching" in raw_crime
                    or "hurto" in raw_crime
                    or "pocket-picking" in raw_crime
                    or "robo" in raw_crime
                    or "obtaining a service without payment" in raw_crime
                    or "purse-snatching" in raw_crime
                    or "false pretenses/swindle/confidence game" in raw_crime
                    or "robbery" in raw_crime
                    or "allanamiento" in raw_crime
                    or ("robo" in raw_crime and "negocio" in raw_crime)
                    or "robo a pasajero" in raw_crime
                    or "robo a transportista" in raw_crime
                    or "vargus" in raw_crime
                    or "pisivargus" in raw_crime
                    or "robatori" in raw_crime
                    or "furto" in raw_crime
                    or "roubo" in raw_crime
                    or "abigeato" in raw_crime
                    or "despojo" in raw_crime
                    or "breakings" in raw_crime
                    or "breaking" in raw_crime
                    and "entering" in raw_crime
                    or "furti" in raw_crime
                    or "rapine" in raw_crime
                    or "強盗" in raw_crime
                    or "金庫破り" in raw_crime
                    or "空き巣" in raw_crime
                    or "自動車盗" in raw_crime
                    or "オートバイ盗" in raw_crime
                    or "自転車盗" in raw_crime
                    or "車上ねらい" in raw_crime
                    or "自販機ねらい" in raw_crime
                    or "工事場ねらい" in raw_crime
                    or "すり" in raw_crime
                    or "ひったくり" in raw_crime
                    or "置引き" in raw_crime
                    or "万引き" in raw_crime
                    or ("非侵入窃盗" in raw_crime and "その他" in raw_crime)
                    or "dacoity" in raw_crime
                    or "house breaking" in raw_crime
                    or "thefts" in raw_crime
                    or "making off from a hotel, restaurant or bar without payment" in raw_crime
                    or "break and enter" in raw_crime
                    or "pickpocketing" in raw_crime
                    or "taking conveyance w/o authority" in raw_crime
                    or "hurtado" in raw_crime
                    or "escalamiento" in raw_crime
                    or "evading fare" in raw_crime
                    or "shop-lifting" in raw_crime
                    or "rb_sorpresa" in raw_crime
                    or "rb_fuerza" in raw_crime
                    or "rb_vehìculo" in raw_crime
                    or "rb_acce_Vehìculo" in raw_crime
                    or "rb_lug_habitado" in raw_crime
                    or "rb_lug_no_habitado" in raw_crime
                    or "otros_rb" in raw_crime
                    or "introduction" in raw_crime
                    or "break & enter" in raw_crime
                    or "a/rob" in raw_crime
                    or "rob" in raw_crime
                    or "steal" in raw_crime
                    or "tacha" in raw_crime
                    or "bmv" in raw_crime
                    or "thft" in raw_crime
                    or (
                            "robbery" in raw_crime
                            or "cambriolage" in raw_crime
                            or "larceny" in raw_crime
                            or "burglary" in raw_crime
                            or "b&e" in raw_crime
                            or "breaking" in raw_crime
                            and "unarmed" in raw_crime
                    )
                    or "apropiació indeguda" in raw_crime
                    or "furt" in raw_crime
                    or "robatori" in raw_crime
                    or "raub" in raw_crime
                    or "diebstahl" in raw_crime
                    or "einbruch" in raw_crime
                    or "被盗" in raw_crime
                    or "盗窃罪" in raw_crime
                    or "抢劫" in raw_crime
                    or "lopások" in raw_crime
                    or "rablások" in raw_crime
                    or "lakásbetörések" in raw_crime
                    or "penadahan" in raw_crime
                    or "pencurian" in raw_crime
                    or "tatvina" in raw_crime
                    or "rob-other" in raw_crime
                    or "shoplift" in raw_crime
                    or "кражи" in raw_crime
                    or "грабежи" in raw_crime
                    or "latrocínio" in raw_crime
                    or "ограбление" in raw_crime
                    or "разбои" in raw_crime
            )
            and "total" not in raw_crime
    ):

        if (
                ("armed" in raw_crime and "unarmed" not in raw_crime)
                or ("force" in raw_crime and "no force" not in raw_crime)
                or "arma" in raw_crime
                or "強盗" in raw_crime
                or "dacoity" in raw_crime
                or "firearms" in raw_crime
                or "violent robbery" in raw_crime
                or "firearms" in raw_crime
                or "pistol" in raw_crime
                or "à main armée" in raw_crime
                or "agresion agravada" in raw_crime
                or "con violencia" in raw_crime
                or "amb violència" in raw_crime
                or "qualifiés" in raw_crime
                or "gun" in raw_crime
                or "knife" in raw_crime
                or "strong" in raw_crime
                or "разбой" in raw_crime
                or "a/" in raw_crime
                or "de cecular" in raw_crime
        ):
            crime = "Robo"
        elif (
                (
                        "auto" in raw_crime
                        or "vehicle" in raw_crime
                        or "car" in raw_crime
                        or "motorcar" in raw_crime
                        or "moped" in raw_crime
                        or "coche" in raw_crime
                        or "carjacking" in raw_crime
                        or "hijacking" in raw_crime
                        or "motocicleta" in raw_crime
                        or "vols de vélos" in raw_crime
                        or "motorcycle" in raw_crime
                        or "vehiculo robado" in raw_crime
                        or "robo de vehiculo" in raw_crime
                        or "robo de vehículo" in raw_crime
                        or "mootorsoiduki" in raw_crime
                        or "jalgratta" in raw_crime
                        or "veículo" in raw_crime
                        or "automezzi" in raw_crime
                        or "ciclomotori" in raw_crime
                        or "motociclo" in raw_crime
                        or "autovetture" in raw_crime
                        or "自動車盗" in raw_crime
                        or "オートバイ盗" in raw_crime
                        or "自転車盗" in raw_crime
                        or "車上ねらい" in raw_crime
                        or "conveyance" in raw_crime
                        or "vehiculo" in raw_crime
                        or "véhicule" in raw_crime
                        or "voiture" in raw_crime
                        or "vehiculo hurtado" in raw_crime
                        or "camion" in raw_crime
                        or "vehicle larceny" in raw_crime
                        or "vehicle theft" in raw_crime
                        or "kendaraan bermotor" in raw_crime
                        or "conveyance" in raw_crime
                        or "transportation" in raw_crime
                        or "veiculo" in raw_crime
                        or "угон тс" in raw_crime
                )
                and "from" not in raw_crime
                and "inside" not in raw_crime
                and "desde" not in raw_crime
                and "da" not in raw_crime
                and "total" not in raw_crime
        ):
            crime = "Robo_de_Coche"
        else:
            crime = "Robo"

    # the script below was added because the crime words in it were not being categorized as Robo_de_Coche if they were not in together
    # with one of the crime words in Robo. I decided to keep both the old script and the new one so the combination of the Robo words and
    # the Robo_de_Coche words and the words below will both be categorized as Robo_de_Coche.

    if (
            (
                    "vehicle grand t" in raw_crime
                    or "vehicle petty t" in raw_crime
                    or "hijacking" in raw_crime
                    or "carjacking" in raw_crime
                    or "vehicle grand theft" in raw_crime
                    or "vehicle petty theft" in raw_crime
                    or "mvt" in raw_crime
                    or "kendaraan bermotor" in raw_crime
                    or "gépkocsi" in raw_crime
                    or "feltörések" in raw_crime
                    or "kraftwagen" in raw_crime
                    or "gta" in raw_crime
                    or "kfz" in raw_crime
                    or "劫持" in raw_crime
                    or "car jack" in raw_crime
                    or "stol veh-passenger vehicle" in raw_crime
                    or "grnd thft" in raw_crime
            )
            and "total" not in raw_crime
    ):
        crime = "Robo_de_Coche"

    if (
            (
                    "assault" in raw_crime
                    or "交通肇事逃逸" in raw_crime
                    or "故意伤害" in raw_crime
                    or "殴打" in raw_crime
                    or "猥亵他人" in raw_crime
                    or "肇事逃逸案" in raw_crime
                    or "uso de fuerza" in raw_crime
                    or "lesiones intencionales" in raw_crime
                    or "asslt" in raw_crime
                    or (
                            "violencia" in raw_crime
                            and "sin" not in raw_crime
                            and "con" not in raw_crime
                    )
                    or "abigeato" in raw_crime
                    or "weapons offense" in raw_crime
                    or "hit & run" in raw_crime
                    or "strangulation" in raw_crime
                    or "obstr breath" in raw_crime
                    or "resist" in raw_crime
                    or "serious injury" in raw_crime
                    or "injury to" in raw_crime
                    or "inj" in raw_crime
                    or "aslt" in raw_crime
                    or "agg assault" in raw_crime
                    or "a&b" in raw_crime
                    or "asalto" in raw_crime
                    or "elder abuse" in raw_crime
                    or "wound" in raw_crime
                    or "knifing" in raw_crime
                    or "stabbing" in raw_crime
                    or "battery" in raw_crime
                    or "bat" in raw_crime
                    or "poison" in raw_crime
                    or "fuerza" in raw_crime
                    or "tortura" in raw_crime
                    or "riot" in raw_crime
                    or "tentativa de feminicidio" in raw_crime
                    or "tentativa de homicidio" in raw_crime
                    or "tentativa de homicídio" in raw_crime
                    or "contra funcionarios publicos" in raw_crime
                    or "violencia física" in raw_crime
                    or "vagivald" in raw_crime
                    or "agressions" in raw_crime
                    or "violència domèstica" in raw_crime
                    or "lesiones dolosas" in raw_crime
                    or "lesiones culposas" in raw_crime
                    or "lesiones" in raw_crime
                    or "les_leves" in raw_crime
                    or "les_graves" in raw_crime
                    or "attentati" in raw_crime
                    or "tentati omicidi" in raw_crime
                    or "lesioni dolose" in raw_crime
                    or "percosse" in raw_crime
                    or "lesão corporal" in raw_crime
                    or "暴行" in raw_crime
                    or "傷害" in raw_crime
                    or "attempt to murder" in raw_crime
                    or ("hurt" in raw_crime and "hurto" not in raw_crime)
                    or "u/s 326 a ipc" in raw_crime
                    or "attempted murder" in raw_crime
                    or "wounding" in raw_crime
                    or "disorder/fighting" in raw_crime
                    or "attempts or threats to murder" in raw_crime
                    or "weapons and explosives offences" in raw_crime
                    or ("impaired adult" in raw_crime and "abuse" in raw_crime)
                    or ("panhandling" in raw_crime and "aggressive" in raw_crime)
                    or "affray" in raw_crime
                    or "shooting" in raw_crime
                    or ("spousal" in raw_crime and "abuse" in raw_crime)
                    or "attempted homicide" in raw_crime
                    or "bodily harm" in raw_crime
                    or "atemptat" in raw_crime
                    or "lesion" in raw_crime
                    or "maltractaments" in raw_crime
                    or "violència" in raw_crime
                    or "körper-verletzungen" in raw_crime
                    or "刺伤" in raw_crime
                    or "伤害" in raw_crime
                    or "突击" in raw_crime
                    or "受伤" in raw_crime
                    or "伤人" in raw_crime
                    or "contre l'intégrité physique" in raw_crime
                    or "violence intrafamiliale physique" in raw_crime
                    or "agresión" in raw_crime
                    or "coups" in raw_crime
                    or "blessures" in raw_crime
                    or "grievous bodily harm" in raw_crime
                    or "simply bodily harm" in raw_crime
                    or "female genital mutilation" in raw_crime
                    or "endangering the life or health" in raw_crime
                    or "endangering life" in raw_crime
                    or "endangering the welfare" in raw_crime
                    or "participation brawl" in raw_crime
                    or "participation attack" in raw_crime
                    or "representations of acts of violence" in raw_crime
                    or "administering substances capable of causing injury to children" in raw_crime
                    # or "weapons" in raw_crime
                    or "against humanity" in raw_crime
                    or "endangering life" in raw_crime
                    or "endangering public safety with weapons" in raw_crime
                    or "endangering the life or health of another/abandonment" in raw_crime
                    or "crimes against persons" in raw_crime
                    or "erőszak" in raw_crime
                    or "penganiayaan" in raw_crime
                    or "offence against a person" in raw_crime
                    or "offensive weapon" in raw_crime
                    or "mvc hit and run" in raw_crime
                    or "traffic - hit and run" in raw_crime
                    or "telesna poškodba" in raw_crime
                    or "nasilje v družini" in raw_crime
                    or "verwondingen" in raw_crime
                    or "deadly weap" in raw_crime
                    or "adw" in raw_crime
                    or "armed dispute" in raw_crime
                    or "fight" in raw_crime
                    or "shot" in raw_crime and (
                            "death" not in raw_crime or "died" not in raw_crime or "dead" not in raw_crime)
                    or "stabbed" in raw_crime and (
                            "death" not in raw_crime or "died" not in raw_crime or "dead" not in raw_crime)
                    or "abuse" in raw_crime
                    or "assualt" in raw_crime
                    or (
                            "display/use" in raw_crime
                            and "weapon" in raw_crime
                            or "wpn" in raw_crime
                            or "gun" in raw_crime
                    )
                    or (
                            "displ/use" in raw_crime
                            and "weapon" in raw_crime
                            or "wpn" in raw_crime
                            or "gun" in raw_crime
                    )
                    or "вред здоровью" in raw_crime
                    or "violence against" in raw_crime
                    or "attempt agg" in raw_crime
                    or ("violence" in raw_crime and "person" in raw_crime)
                    or "тяжкие телесные повреждения" in raw_crime
            )
            and "total" not in raw_crime
            and "domest" not in raw_crime
            and "dating" not in raw_crime
    ):
        crime = "Asalto"

    if (
            (
                    "extorcion" in raw_crime
                    or "extorcion" in raw_crime
                    or "exhortos" in raw_crime
                    or "extortion" in raw_crime
                    or "coercion" in raw_crime
                    or "extorsion" in raw_crime
                    or "extorción" in raw_crime
                    or "extorsão" in raw_crime
                    or "estorsioni" in raw_crime
                    or "敲诈勒索" in raw_crime
                    or "coaccions" in raw_crime
                    or "extorsió'" in raw_crime
                    or "敲诈" in raw_crime
                    or "extorsión" in raw_crime
                    or "izsiljevanje" in raw_crime
                    or "вымогательство" in raw_crime
            )
            and "total" not in raw_crime
    ):
        crime = "Extorcion"

    if (
            (
                    "kidnap" in raw_crime
                    or "imprisonment" in raw_crime
                    or "desaparicion forzada" in raw_crime
                    or "abduct" in raw_crime
                    or "secuestro" in raw_crime
                    or "plagio" in raw_crime
                    or "restraint" in raw_crime
                    or "rapto" in raw_crime
                    or "purchase of a child" in raw_crime
                    or "privacion de la libertad" in raw_crime
                    or "privación ilegal de libertad" in raw_crime
                    or "trafficking of person" in raw_crime
                    or "trafficking" in raw_crime
                    or "retención" in raw_crime
                    or "sustracc" in raw_crime
                    or "trafico de infantes" in raw_crime
                    or "involuntary servitude" in raw_crime
                    or "robo de infante" in raw_crime
                    or "roovimine" in raw_crime
                    or "sequestro" in raw_crime
                    or "sequestri" in raw_crime
                    or "abduction" in raw_crime
                    or "trata humana" in raw_crime
                    or "trata de personas" in raw_crime
                    or "segrest" in raw_crime
                    or "sostracció de menor" in raw_crime
                    or "freiheits-beraubung" in raw_crime
                    or "拐" in raw_crime
                    or "enlèvement" in raw_crime
                    or "desaparecidos" in raw_crime
                    or "autosecuestro" in raw_crime
                    or "hostage taking" in raw_crime
                    or "penculikan" in raw_crime
                    or "odvzem prostosti" in raw_crime
                    or "desaparición forzada" in raw_crime
            )
            and "total" not in raw_crime
    ):
        crime = "Secuestro"

    if (
            (
                    "organized" in raw_crime
                    or "blackmail" in raw_crime
                    or "conspiracy" in raw_crime
                    or "usury" in raw_crime
                    or "crimen organizado" in raw_crime
                    or "lavado de activos" in raw_crime
                    or "receptación" in raw_crime
                    or "sicariato" in raw_crime
                    or "tráfico de influencias" in raw_crime
                    or "tráfico de moneda" in raw_crime
                    or "tráfico de órganos" in raw_crime
                    or "tráfico ilícito de" in raw_crime
                    or "money launder" in raw_crime
                    or "operaciones con recursos de procedencia ilicita" in raw_crime
                    or "operating/promoting/assisting gambling" in raw_crime
                    or "pandilla" in raw_crime
                    or "mafioso" in raw_crime
                    or "riciclaggio" in raw_crime
                    or "keeping vice establishments" in raw_crime
                    or "crime organisé" in raw_crime
                    or "gang" in raw_crime
                    or "criminal organisation" in raw_crime
                    or "blanqueig" in raw_crime
                    or (
                            "grups" in raw_crime
                            or "organitzacions" in raw_crime
                            and "criminals" in raw_crime
                    )
                    or "pranje denarja" in raw_crime
                    or "illicit manufacturing" in raw_crime
                    or "sale/manufacture" in raw_crime
                    or "manufacture / deliver" in raw_crime
                    or "asociación ilícita" in raw_crime
                    or "delincuencia organizada" in raw_crime
            )
            and "total" not in raw_crime
    ):
        crime = "Actividad_de_Crimen_Organizado"

    if (
            (
                    (
                            "sex" in raw_crime and "sex offender registration viol" not in raw_crime and "sexies" not in raw_crime and
                            "failure to register" not in raw_crime)
                    or "viol." in raw_crime
                    or "sexual" in raw_crime
                    or "contact-sexual" in raw_crime
                    or "rape" in raw_crime
                    or "incest" in raw_crime
                    or "violacion" in raw_crime
                    or "indec expo" in raw_crime
                    or "restraining order violation" in raw_crime
                    or "violación" in raw_crime
                    or "groped" in raw_crime
                    or "sodomy" in raw_crime
                    or "fondling" in raw_crime
                    or "batt/sexual" in raw_crime
                    or "batt/oral" in raw_crime
                    or "criminal solicitation" in raw_crime
                    or "bestiality" in raw_crime
                    or ("indecency" in raw_crime and "child" in raw_crime)
                    or "prostitución" in raw_crime
                    or "escort" in raw_crime
                    or "proxenetismo" in raw_crime
                    or "utilización de personas" in raw_crime
                    or "prostitution" in raw_crime
                    or "estupro" in raw_crime
                    or "violacion equiparada" in raw_crime
                    or "lenocinio" in raw_crime
                    or "incesto" in raw_crime
                    or "sessuali" in raw_crime
                    or "minorenne" in raw_crime
                    or "molestation" in raw_crime
                    or "rape(1)minor" in raw_crime
                    or "posco act" in raw_crime
                    or "u/s 354 ipc" in raw_crime
                    or "u/s 326b ipc" in raw_crime
                    or "intent to dosrobe" in raw_crime
                    or "indecent assault" in raw_crime
                    or "unnatural offences" in raw_crime
                    or "other offences vs. public morality" in raw_crime
                    or "child abuse" in raw_crime
                    or "massage or erogenous areas" in raw_crime
                    or "obscene material" in raw_crime
                    or ("photography" in raw_crime and "minor" in raw_crime)
                    or "prostitute" in raw_crime
                    or "sexuals" in raw_crime
                    or "卖淫" in raw_crime
                    or "violence sexuelle" in raw_crime
                    or "prostitució" in raw_crime
                    or ("porno" in raw_crime and "menors" in raw_crime)
                    or "csc - penetrate with object" in raw_crime
                    or "perkosaan" in raw_crime
                    or "kršitev" in raw_crime
                    or "posilstvo" in raw_crime
                    or "spoln" in raw_crime
                    or "nasilništvo" in raw_crime
                    or "lewd" in raw_crime
                    or "pimping" in raw_crime
                    or "soliciting" in raw_crime
                    or "pencabulan" in raw_crime
                    or "изнасилование" in raw_crime
            )
            and "total" not in raw_crime
            and "domest" not in raw_crime
    ):
        crime = "Violacion"

    if (
            (
                    "arson" in raw_crime
                    or "grafitti" in raw_crime
                    or "criminal mis" in raw_crime
                    or "graffiti" in raw_crime
                    or "grafitt" in raw_crime
                    or "mischief" in raw_crime
                    or "damage" in raw_crime
                    or "damage city" in raw_crime
                    or "damage prop" in raw_crime
                    or "damage to" in raw_crime
                    or "vandalism" in raw_crime
                    or "vandalismo" in raw_crime
                    or "defacing" in raw_crime
                    or "defacement" in raw_crime
                    or "causing a flood, collapse or landslide" in raw_crime
                    or "causing explosion" in raw_crime
                    or "vandal" in raw_crime
                    or "dumping complaint" in raw_crime
                    or "vand" in raw_crime
                    or "dano" in raw_crime
                    or ("daño" in raw_crime and "intencional" in raw_crime)
                    or "daño a vias" in raw_crime
                    or "sabotaje" in raw_crime
                    or (
                            "daño" in raw_crime
                            and "propiedad" in raw_crime
                            or "materiales" in raw_crime
                    )
                    or "actes contra la propietat" in raw_crime
                    or "vandalisme" in raw_crime
                    or "criminal damage" in raw_crime
                    or "danneggiamenti" in raw_crime
                    or "incendio" in raw_crime
                    or "desecration" in raw_crime
                    or "burning" in raw_crime
                    or "firebombing" in raw_crime
                    or "property crime" in raw_crime
                    or "故意损坏公私财物" in raw_crime
                    or "故意损毁" in raw_crime
                    or "明火" in raw_crime
                    or "消防安全" in raw_crime
                    or "过失引起火灾" in raw_crime
                    or "过失引起火灾案" in raw_crime
                    or "danys" in raw_crime
                    or "incendi" in raw_crime
                    or "brand" in raw_crime
                    or "graffiti" in raw_crime
                    or "incendi" in raw_crime
                    or "损伤" in raw_crime
                    or "dégradation de la propriété" in raw_crime
                    or "méfait" in raw_crime
                    or "crimes against property" in raw_crime
                    or "rongalas" in raw_crime
                    or "tulajdon elleni szabálysértések" in raw_crime
                    or "pembakaran" in raw_crime
                    or "pengrusakan" in raw_crime
                    or "poškodovanje" in raw_crime
                    or "uničenje" in raw_crime
                    or "destruct" in raw_crime
                    or "повреждение" in raw_crime
                    or "destrucción de bienes" in raw_crime
            )
            and "total" not in raw_crime
    ):
        crime = "Vandalismo"

    if (
            (
                    "narcotic" in raw_crime
                    or "吸毒" in raw_crime
                    or "吸食毒" in raw_crime
                    or "非法持有毒品" in raw_crime
                    or "tráfico" in raw_crime
                    or "siembra" in raw_crime
                    or "distrib" in raw_crime
                    or "narc" in raw_crime
                    or "controlled substance" in raw_crime
                    or "marijuana" in raw_crime
                    or "amphetamine" in raw_crime
                    or "meth " in raw_crime
                    or "smuggling" in raw_crime
                    or "poss." in raw_crime
                    or "poss of" in raw_crime
                    or "possession" in raw_crime
                    or "controlled sub" in raw_crime
                    or "materias ilegales" in raw_crime
                    or "estupefacientes" in raw_crime
                    or "possess" in raw_crime
                    or "cont.sub" in raw_crime
                    or "contraband" in raw_crime
                    or "drug" in raw_crime
                    or "overdose" in raw_crime
                    or "estupefaents" in raw_crime
                    or "psicotropics" in raw_crime
                    or "entorpecentes" in raw_crime
                    or "tráfico" in raw_crime
                    or "narcomenudeo" in raw_crime
                    or "contrabbando" in raw_crime
                    or "stupefacenti" in raw_crime
                    or "sale of obscene object/s" in raw_crime
                    or "manufacturing of d.d." in raw_crime
                    or "trafficking in d.d." in raw_crime
                    or "contraband" in raw_crime
                    or "cannab" in raw_crime
                    or "pwits" in raw_crime
                    or "p-w-i-t-s" in raw_crime
                    or "cocaine" in raw_crime
                    or "heroin" in raw_crime
                    or "marijuana" in raw_crime
                    or "fentanyl" in raw_crime
                    or "crack" in raw_crime
                    or "peddling" in raw_crime
                    or "drogue" in raw_crime
                    or "rauschgift" in raw_crime
                    or "trafic" in raw_crime
                    or "stupéfiants" in raw_crime
                    or "crimes against  alcoholic drinks law" in raw_crime
                    or "obat" in raw_crime
                    or "narkotika" in raw_crime
                    or "promet" in raw_crime
                    or "izdelovanje" in raw_crime
                    or "proizvodnja" in raw_crime
                    or "cont subs" in raw_crime
                    or "toxic substances" in raw_crime
                    or "hallucin" in raw_crime
                    or "opiate" in raw_crime
                    or "fabricac" in raw_crime
                    or "opium" in raw_crime
                    or "poss:" in raw_crime
                    or "наркопреступления" in raw_crime
            )
            and "loitering" not in raw_crime
            and "weapon" not in raw_crime
            and "driving" not in raw_crime
            and "radio" not in raw_crime
            and "stolen" not in raw_crime
            and "gambling" not in raw_crime
            and "fireworks" not in raw_crime
            and "under the influ" not in raw_crime
            and "driving" not in raw_crime
            and "escolar" not in raw_crime
            and "pesado" not in raw_crime
            and "orožja" not in raw_crime
            and "nedovoljena" not in raw_crime
            and "total" not in raw_crime
    ):
        crime = "Trafico_de_Materias_Ilegales"

    if (
            (
                    "fraud" in raw_crime
                    or "defraudacion" in raw_crime
                    or "defraudación" in raw_crime
                    or "fraudulenta" in raw_crime
                    or "wothless" in raw_crime
                    or "usura" in raw_crime
                    or "issuing a false medical certificate" in raw_crime
                    or "maliciously causing financial loss to another" in raw_crime
                    # california welfare fraud code
                    or "w&i" in raw_crime
                    or "worthless" in raw_crime
                    or "white-collar" in raw_crime
                    or "captac" in raw_crime
                    or "forge" in raw_crime
                    or "counterfei" in raw_crime
                    or "reduction of assets to the prejudice of creditors" in raw_crime
                    or "removal of property" in raw_crime
                    or "fraude" in raw_crime
                    or "misuse without criminal intent or through negligence"
                    in raw_crime  # TODO double check this
                    or "accepting an advantage" in raw_crime
                    or "reduction of assets to the prejudice of creditors" in raw_crime
                    or "unlawful use of financial assets" in raw_crime
                    or "embezz" in raw_crime
                    or "perj" in raw_crime
                    or "embezzlement" in raw_crime
                    or "trafico de influencia" in raw_crime
                    or "usurpación de identidad" in raw_crime
                    or "overcharging of taxes" in raw_crime
                    or "abuse of public office" in raw_crime
                    or "failure to comply with accounting regulations" in raw_crime
                    or "misconduct in public office" in raw_crime
                    or "abuso de confianza" in raw_crime
                    or "suplantación de identidad" in raw_crime
                    or "enriquecimiento ilicito" in raw_crime
                    or "identity th" in raw_crime
                    or "documento falso" in raw_crime
                    or "falsedad" in raw_crime
                    or "falsificación" in raw_crime
                    or "falsificacion" in raw_crime
                    or "alteracion" in raw_crime
                    or "alteración" in raw_crime
                    or "kelmus" in raw_crime
                    or "estelionato" in raw_crime
                    # or "latrocínio" in raw_crime
                    or "truffe" in raw_crime
                    or "contraffazione" in raw_crime
                    or "詐欺" in raw_crime
                    or "deception" in raw_crime
                    or "deceptive" in raw_crime
                    or "forgery" in raw_crime
                    or "apropiacion ilegal" in raw_crime
                    or "alter" in raw_crime
                    or ("bad" in raw_crime and "check" in raw_crime)
                    or "scalping" in raw_crime
                    or "scam" in raw_crime
                    or "伪" in raw_crime
                    or "伪造的" in raw_crime
                    or "使用伪造" in raw_crime
                    or "使用变造的" in raw_crime
                    or "使用变造证" in raw_crime
                    or "冒用" in raw_crime
                    or "招" in raw_crime
                    or "提供虚假证言" in raw_crime
                    or "诈骗" in raw_crime
                    or "estafes" in raw_crime
                    or "defraudaci" in raw_crime
                    or "falsificació" in raw_crime
                    or "usurpació" in raw_crime
                    or "伪造" in raw_crime
                    or "escroquerie" in raw_crime
                    or "cobro ilegal" in raw_crime
                    or "estafa" in raw_crime
                    or "penipuan" in raw_crime
                    or "na črno" in raw_crime
                    or "goljufija" in raw_crime
                    or "ponareditev" in raw_crime
                    or "davčna zatajitev" in raw_crime
                    or "bedrog" in raw_crime
                    or "false info" in raw_crime
                    or "false police reports" in raw_crime
                    or "false pretenses" in raw_crime
                    or "ponzi" in raw_crime
                    or "taking identity of another" in raw_crime
                    or "unlawful use of financial card" in raw_crime
                    or (
                            "false" in raw_crime
                            or "falsify" in raw_crime
                            and "id" in raw_crime
                            or "document" in raw_crime
                            or "news" in raw_crime
                            or "statement" in raw_crime
                            or "credit" in raw_crime
                            or "personation" in raw_crime
                            or "identification" in raw_crime
                    )
                    or "penggelapan" in raw_crime
                    or "engaño" in raw_crime
                    or "concusión" in raw_crime
                    or "evasión" in raw_crime
                    or "aprovechamiento ilícito" in raw_crime
                    or "enriquecimiento ilícito" in raw_crime
                    or "testaferrismo" in raw_crime
                    or "agiotaje" in raw_crime
            )
            and "total" not in raw_crime
    ):
        crime = "Fraude"

    if (
            (
                    "murder" in raw_crime
                    or "homicide" in raw_crime
                    or "homicidio" in raw_crime
                    or "femicidio" in raw_crime
                    or "manslaughter" in raw_crime
                    or "genocidio" in raw_crime
                    or "assassinat" in raw_crime
                    or "maurtre" in raw_crime
                    or "muerte culposa" in raw_crime
                    or "genocide" in raw_crime
                    or "feminicidio" in raw_crime
                    or "atemptats" in raw_crime
                    or "homicídio" in raw_crime
                    or "strage" in raw_crime
                    or "omicidi" in raw_crime
                    or "infantici" in raw_crime
                    or "kill" in raw_crime
                    or "dowry death" in raw_crime
                    or "asesinato" in raw_crime
                    or "death investigation" in raw_crime
                    or "assassinat" in raw_crime
                    or "homicidi dolós" in raw_crime
                    or "杀" in raw_crime
                    or "死亡" in raw_crime
                    or "infractions entrainant la mort" in raw_crime
                    or "pembunuhan" in raw_crime
                    or "moord" in raw_crime
                    or "doodslag" in raw_crime
                    or "убийства" in raw_crime
                    or "feminicídio" in raw_crime
                    or "fatally shot" in raw_crime
                    or "died after shots fired" in raw_crime
                    or "shot" in raw_crime and ("death" in raw_crime or "died" in raw_crime or "dead" in raw_crime)
                    or "stabbed" in raw_crime and ("death" in raw_crime or "died" in raw_crime or "dead" in raw_crime)

            )
            and "attempt to murder" not in raw_crime
            and "tentativa de homicidio" not in raw_crime
            and "tentativa de homicídio" not in raw_crime
            and "tentati omicidi" not in raw_crime
            and "attempted murder" not in raw_crime
            and "attempts or threats to murder" not in raw_crime
            and "attempted homicide" not in raw_crime
            and "total" not in raw_crime
    ):
        crime = "Homicidio"

    if (
            (
                    "terror" in raw_crime
                    or "terrorismo" in raw_crime
                    or "terrorist attack" in raw_crime
                    or "terrorisme" in raw_crime
                    or "恐怖主义" in raw_crime
                    or "attentats" in raw_crime
            )
            and "terroristic threat" not in raw_crime
            and "total" not in raw_crime
    ):
        crime = "Terrorismo"

    if (
            (
                    "corruption" in raw_crime
                    or "cohecho" in raw_crime
                    or "abuso de autoridad" in raw_crime
                    or "encubrimiento" in raw_crime
                    or "abuso de autoridad" in raw_crime
                    or "ejercicio indebido" in raw_crime
                    or "ejercicio abusivo" in raw_crime
                    or "ejercicio ilegal" in raw_crime
                    or "financial exploitation" in raw_crime
                    or "bribery" in raw_crime
                    or "tampering" in raw_crime
                    or "abuse of official" in raw_crime
                    or "corrupción" in raw_crime
                    or "breach of a prohibition from practising a profession" in raw_crime
                    or "uso indebido de atribuciones y facultades" in raw_crime
                    or "usurpacion de funciones publicas" in raw_crime
                    or "coaccion" in raw_crime
                    or "delitos de abogados, patronos, litigantes y asesores juridicos" in raw_crime
                    or "uso indebido de insignias y uniformes" in raw_crime
                    or "impersonat" in raw_crime
                    or "corrupcion" in raw_crime
                    or "peculado" in raw_crime
                    or "corrupção" in raw_crime
                    or "delitos cometidos por servidores públicos" in raw_crime
                    or "占有離脱物横領" in raw_crime
                    or "bribe" in raw_crime
                    or "brib" in raw_crime
                    or "suborn" in raw_crime
                    or "贿赂" in raw_crime
                    or "granting an advantage" in raw_crime
                    or "crimes against public post" in raw_crime
                    or "poneverba" in raw_crime
                    or "zloraba" in raw_crime
                    or "nedovoljeno sprejemanje daril" in raw_crime
                    or "nedovoljeno dajanje daril" in raw_crime
                    or "korupsi" in raw_crime
            )
            and "total" not in raw_crime
    ):
        crime = "Corrupción"

    if "violence and sexual offences" in raw_crime:
        crime = "Violent_Crimes"

    if ("凶悪犯" in raw_crime and "その他" in raw_crime) or "凶器準備集合" in raw_crime:
        crime = "Violent_Crimes"

    if (
            "openlijke geweldpleging" in raw_crime
            or "public violence" in raw_crime
            or "opzettelijke slagen en verwondingen" in raw_crime
            or "doodslag" in raw_crime
            or "agressieve diefstal" in raw_crime
            or "diefstal met gebruik of vertoon van een wapen" in raw_crime
            or "opzettelijke brandstichting" in raw_crime
            or "geweld" in raw_crime
    ):
        crime = "Violent_Crimes"

    # Netherlands - burglary types
    if (
            "huisdiefstal" in raw_crime
            or "woninginbraken" in raw_crime
            or "handelszaken inbraken" in raw_crime
            or "andere inbraken" in raw_crime
            or "diefstal met geweld" in raw_crime
            or "gebruiksdiefstal" in raw_crime
            or "inbraken bedrijf" in raw_crime
    ):
        crime = "Robo"

    # attempted burglary
    if "pogingen tot woninginbraak" in raw_crime:
        crime = "Robo"

    # Netherlands - other types of robbery
    if (
            "enkelvoudige of gewone diefstal" in raw_crime
            or "gauwdiefstal" in raw_crime
            or "winkeldiefstal" in raw_crime
            or "fietsdiefstal" in raw_crime
            or "diefstal uit voertuig" in raw_crime
            or "handtasroof" in raw_crime
            or "zakkenrollerij" in raw_crime
            or "overval" in raw_crime
            or "straatroof" in raw_crime
            or "diefstal" in raw_crime
            or "diefstallen" in raw_crime
    ):
        crime = "Robo"

    # Netherlands - motorvehical theft
    # "autodiefstal": "car theft",
    # "diefstal motorfiets": "motorcycle theft",
    # "diefstal bromfiets": "moped theft",
    if (
            "autodeifstal" in raw_crime
            or "diefstal motorfiets" in raw_crime
            or "diefstal bromfiets" in raw_crime
            or "motorvoertuigen" in raw_crime
            or "brom-" in raw_crime
            or "snor-" in raw_crime
            or "fietsen" in raw_crime
            or "voertuigen" in raw_crime
    ):
        crime = "Robo_de_Coche"

    # Netherlands crime = "Trafico_de_Materias_Ilegales"
    if (
            "drugs" in raw_crime
            or "drugshandel" in raw_crime
            or "druggebruik" in raw_crime
            or "drugsbezit" in raw_crime
            or "drugsaanmaak" in raw_crime
            or "drugszoekgedrag" in raw_crime
            or "heling" in raw_crime
    ):
        crime = "Trafico_de_Materias_Ilegales"

    # Netherlands fraud crimes
    if (
            "oplichting" in raw_crime
            or "bedriegerij" in raw_crime
            or "valsheid in geschriften" in raw_crime
            or "valse munt" in raw_crime
            or "namaking/vervalsing" in raw_crime
            or "misbruik van vertrouwen" in raw_crime
    ):
        crime = "Fraude"

    # Netherlands Actividad_de_Crimen_Organizado crimes
    if "bedrijven" in raw_crime:
        crime = "Actividad_de_Crimen_Organizado"

    # Netherlands Violacion crimes
    if (
            "aanmatiging" in raw_crime
            or "andere misdrijven tegen de openbare trouw" in raw_crime
            or "flessentrekkerij" in raw_crime
            or "zedenfeiten" in raw_crime
            or "sluiksort" in raw_crime
            or "geluidshinder" in raw_crime
            or "verboden wapens" in raw_crime
            or "racisme/discriminatie" in raw_crime
            or "mishandeling" in raw_crime
            or "zedenmisdrijf" in raw_crime
    ):
        crime = "Violacion"

    if "bedreiging" in raw_crime or "bedreigingen" in raw_crime:
        crime = "Acoso"

    if "vandalisme" in raw_crime:
        crime = "Vandalismo"

    # autokraak: car squatting
    if "autokraak" in raw_crime:
        crime = "Otro"

    # Minneapolis assault categories
    if (
            "aslt-sgnfcnt bdly hm" in raw_crime
            or "aslt-great bodily hm" in raw_crime
            or "aslt4-less than subst harm" in raw_crime
    ):
        crime = "Asalto"

    if (
            ("theft" in raw_crime
             or "盗窃" in raw_crime
             or "Vol" in raw_crime
             or "Industrial espionage" in raw_crime
             or "b&e" in raw_crime
             or "burgl" in raw_crime
             or "burglar" in raw_crime
             or "burglary" in raw_crime
             or "burg" in raw_crime
             or "b&" in raw_crime
             or "stole" in raw_crime
             or "shoplifting" in raw_crime
             or "larceny" in raw_crime
             or "larc" in raw_crime
             or "hurto" in raw_crime
             or "pocket-picking" in raw_crime
             or "obtaining a service without payment" in raw_crime
             or "false pretenses/swindle/confidence game" in raw_crime
             or "vargus" in raw_crime
             or "pisivargus" in raw_crime
             or "furto" in raw_crime
             or "roubo" in raw_crime
             or "abigeato" in raw_crime
             or "breakings" in raw_crime
             or "breaking" in raw_crime
             or "entering" in raw_crime
             or "furti" in raw_crime
             or "金庫破り" in raw_crime
             or "空き巣" in raw_crime
             or "自販機ねらい" in raw_crime
             or "工事場ねらい" in raw_crime
             or "すり" in raw_crime
             or "置引き" in raw_crime
             or "万引き" in raw_crime
             or "非侵入窃盗 AND その他" in raw_crime
             or "house breaking" in raw_crime
             or "thefts" in raw_crime
             or "making off from a hotel, restaurant or bar without payment" in raw_crime
             or "break and enter" in raw_crime
             or "pickpocketing" in raw_crime
             or "taking conveyance w/o authority" in raw_crime
             or "hurtado" in raw_crime
             or "escalamiento" in raw_crime
             or "evading fare" in raw_crime
             or "shop-lifting" in raw_crime
             or "rb_sorpresa" in raw_crime
             or "rb_lug_habitado" in raw_crime
             or "rb_lug_no_habitado" in raw_crime
             or "otros_rb" in raw_crime
             or "introduction" in raw_crime
             or "break & enter" in raw_crime
             or "rob" in raw_crime
             or "Steal" in raw_crime
             or "bmv" in raw_crime
             or "thft" in raw_crime
             or "apropiació indeguda" in raw_crime
             or "furt" in raw_crime
             or "diebstahl" in raw_crime
             or "einbruch" in raw_crime
             or "被盗" in raw_crime
             or "盗窃罪" in raw_crime
             or "lopások" in raw_crime
             or "lakásbetörések" in raw_crime
             or "pencurian" in raw_crime
             or "tatvina" in raw_crime
             or "shoplift" in raw_crime
             or "кражи" in raw_crime)
            and "total" not in raw_crime
    ):
        crime = "Hurto"

        if (
                ("armed" in raw_crime
                 or "force" in raw_crime
                 or "arma" in raw_crime
                 or "強盗" in raw_crime
                 or "firearms" in raw_crime
                 or "violent robbery" in raw_crime
                 or "firearms" in raw_crime
                 or "pistol" in raw_crime
                 or "à main armée" in raw_crime
                 or "agresion agravada" in raw_crime
                 or "con violencia" in raw_crime
                 or "amb violència" in raw_crime
                 or "qualifiés" in raw_crime
                 or "gun" in raw_crime
                 or "knife" in raw_crime
                 or "strong" in raw_crime
                 or "разбой" in raw_crime
                 or "a/" in raw_crime)
                and "unarmed" not in raw_crime
                and "no force" not in raw_crime
        ):
            crime = "Robo"

    if (
            ("snatch" in raw_crime
             or "snatching" in raw_crime
             or "robo" in raw_crime
             or "purse-snatching" in raw_crime
             or "robbery" in raw_crime
             or "robo a pasajero" in raw_crime
             or "robo a transportista" in raw_crime
             or "robatori" in raw_crime
             or "rapina" in raw_crime
             or "despojo" in raw_crime
             or "rapine" in raw_crime
             or "強盗" in raw_crime
             or "ひったくり" in raw_crime
             or "dacoity" in raw_crime
             or "rb_fuerza" in raw_crime
             or "a/rob" in raw_crime
             or "raub" in raw_crime
             or "抢劫" in raw_crime
             or "rablások" in raw_crime
             or "penadahan" in raw_crime
             or "rob-other" in raw_crime
             or "грабежи" in raw_crime
             or "latrocinio" in raw_crime
             or "oграбление" in raw_crime
             or "pазбои" in raw_crime)
            and "total" not in raw_crime
    ):
        crime = "Robo"

    if (
            ("auto" in raw_crime
             or "auto theft" in raw_crime
             or "gta" in raw_crime
             or "vehicle" in raw_crime
             or "car" in raw_crime
             or "motorcar" in raw_crime
             or "moped" in raw_crime
             or "coche" in raw_crime
             or "carjacking" in raw_crime
             or "hijacking" in raw_crime
             or "motocicleta" in raw_crime
             or "vols de vélos" in raw_crime
             or "motorcycle" in raw_crime
             or "vehiculo robado" in raw_crime
             or "robo de vehiculo" in raw_crime
             or "robo de vehículo" in raw_crime
             or "mootorsoiduki" in raw_crime
             or "jalgratta" in raw_crime
             or "veículo" in raw_crime
             or "automezzi" in raw_crime
             or "ciclomotori" in raw_crime
             or "motociclo" in raw_crime
             or "autovetture" in raw_crime
             or "自動車盗" in raw_crime
             or "オートバイ盗" in raw_crime
             or "自転車盗" in raw_crime
             or "車上ねらい" in raw_crime
             or "conveyance" in raw_crime
             or "vehiculo" in raw_crime
             or "véhicule" in raw_crime
             or "voiture" in raw_crime
             or "vehiculo hurtado" in raw_crime
             or "camion" in raw_crime
             or "vehicle larceny" in raw_crime
             or "vehicle theft" in raw_crime
             or "kendaraan bermotor" in raw_crime
             or "conveyance" in raw_crime
             or "transportation" in raw_crime
             or "veiculo" in raw_crime
             or "rb_vehìculo" in raw_crime
             or "rb_acce_Vehìculo" in raw_crime
             or "угон тс" in raw_crime)
            and "total" not in raw_crime
    ):
        crime = "Robo_de_Coche"

        if (
                "from" in raw_crime
                and "inside" in raw_crime
                and "desde" in raw_crime
                and "da" in raw_crime
        ):
            crime = "Hurto"

    if (
            (
                    "susp" in raw_crime
                    or ("susp" in raw_crime and "veh" in raw_crime)
                    or "loitering" in raw_crime
                    or "sospech" in raw_crime
            )
            and "bat" not in raw_crime
            and "gang" not in raw_crime
            and "suspect" not in raw_crime
            and "suspicion" not in raw_crime
            and "card" not in raw_crime
            and "shoot" not in raw_crime
            and "stab" not in raw_crime
    ):
        crime = "Actividad_Sospechosa"

    if (
            (
                    "disturb" in raw_crime
                    or "drunk" in raw_crime
                    or ("public" in raw_crime and "order" in raw_crime)
                    or "noisy" in raw_crime
                    or "breach of the peace" in raw_crime
                    or "intox" in raw_crime
                    or "desorden publico" in raw_crime
            )
            and "domest" not in raw_crime
            and "bat" not in raw_crime
            and "driver" not in raw_crime
            and "driving" not in raw_crime
            and "vehicle" not in raw_crime
            and "highway" not in raw_crime
            and "road" not in raw_crime
            and "alley" not in raw_crime
            and "shoot" not in raw_crime
            and "stab" not in raw_crime
            and ("violat" not in raw_crime or "violent" not in raw_crime)
            and "auto" not in raw_crime
            and "armed" not in raw_crime
            and "assault" not in raw_crime
            and "med" not in raw_crime
    ):
        crime = "Desorden"

    if (
            (
                    ("driver" in raw_crime or "driving" in raw_crime)
                    or "dui" in raw_crime
                    or "dwi" in raw_crime
                    or ("traffic" in raw_crime and "violat" in raw_crime)
                    or ("impair" in raw_crime and "driv" in raw_crime)
                    or ("intox" in raw_crime and "driv" in raw_crime)
                    or ("drunk" in raw_crime and "driv" in raw_crime)
                    or
                    (
                            "borrach" in raw_crime and "manejando" in raw_crime
                            or "guiando" in raw_crime
                            or "volante" in raw_crime
                    )
            )
            and "forger" not in raw_crime
            and "robbery" not in raw_crime
            and "assault" not in raw_crime
            and "shoot" not in raw_crime
            and "stab" not in raw_crime
            and "stole" not in raw_crime
            and "trafficking" not in raw_crime
            and "abuse" not in raw_crime
            and "goodwill" not in raw_crime
    ):
        crime = "Violaciones_de_Transito"

    if (
            (
                    "domest" in raw_crime
                    or ("violen" in raw_crime and "dat" in raw_crime)
                    or "violencia psicológica contra la mujer o miembros del núcleo familiar" in raw_crime
                    or "domes aslt" in raw_crime
            )
            and "homicid" not in raw_crime
    ):
        crime = "Ofensas_Domesticas"

    return crime