# Load libraries
from efficient_apriori import apriori
import pandas as pd
import pickle
import json
import argparse
import os
import time

# ==================================================== #
#                Command line Argument                 #
# ==================================================== #
parser = argparse.ArgumentParser(description='Project.')
parser.add_argument('-all',
                    help='Run all steps after first defined step (-step n). If set to false it will only run one step',
                    default=False,
                    action="store_true")
parser.add_argument('-step',
                    help='First step to run',
                    default=0)
parser.add_argument('-only-steps',
                    help='Runs specified steps only. NOTE: The run order will always be crescent step number.',
                    default='')
parser.add_argument('-interact',
                    help='Run interactive: ask for whats steps to run',
                    default=False,
                    action="store_true")
parser.add_argument('-json',
                    help='Serialize using json format?',
                    default=False,
                    action="store_true")
parser.add_argument('-i',
                    help='Define input file.',
                    default="data\\input-sm.csv")
parser.add_argument('-o',
                    help='Define output directory.',
                    default="data\\")
parser.add_argument('-q',
                    help='Run quiet mode (no debug printing)',
                    default=False,
                    action="store_true")
args = parser.parse_args()

if args.interact:
    args.only_steps = input('Enter steps to run [may enter multiple separated by comma]: ')

if not (os.path.exists(args.o)) or not (os.path.isdir(args.o)):
    os.mkdir(args.o)

if args.o != '' and not (args.o.endswith('\\')):
    args.o = args.o + "\\"


# ==================================================== #
#                  Func definitions                    #
# ==================================================== #

# Should step with this Id be run?
def do_run_step(step, quiet=False):
    run = False
    if args.only_steps == '':
        if bool(args.all) and int(args.step) <= step:
            run = True
        elif str(args.step) == str(step):
            run = True
    else:
        if args.only_steps.split(',').__contains__(str(step)):
            run = True

    if not quiet:
        if run:
            log('Running step ' + str(step))
        else:
            log('Skipping step ' + str(step))

    return run


# Print Debug information
tim = {'start': time.time()}
def log(msg):
    if not args.q:
        tmp = time.time()
        print("[" + "{:10.3f}".format(tmp - tim['start']) + "s] " + msg)
        tim['start'] = tmp

# TODO Remove
def save_csv_dict(step, dic_of_list, sufix=""):
    with open(args.o + "step_" + str(step) + sufix + ".csv", "w") as f:
        for key in dic_of_list.keys():
            f.write(str(key) + ';' + ";".join(str(x) for x in dic_of_list[key]) + '\n')


# TODO Remove
def save_csv(step, prescriptions, sufix=""):
    with open(args.o + "step_" + str(step) + sufix + ".csv", "w") as f:
        for presc in prescriptions:
            f.write(";".join(str(x) for x in list) + '\n')

def save_apriori_result_csv(step, results, diag='', sufix=""):
    with open(args.o + "step_" + str(step) + sufix + ".csv", "w") as f:
        if isinstance(results, dict):
            f.write(";".join(['Diagnosis', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']) + '\n')
            for diag in results.keys():
                for r in results[diag]:
                    f.write(";".join([
                        diag,
                        str(r.lhs),
                        str(r.rhs),
                        str(r.confidence),
                        str(r.support),
                        str(r.lift),
                        str(r.conviction)
                    ]) + '\n')
        elif diag != '':
            f.write(";".join(['Diagnosis', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']) + '\n')
            for r in results:
                f.write(";".join([
                    diag,
                    str(r.lhs),
                    str(r.rhs),
                    str(r.confidence),
                    str(r.support),
                    str(r.lift),
                    str(r.conviction)
                ]) + '\n')
        else:
            f.write(";".join(['LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']) + '\n')
            for r in results:
                f.write(";".join([
                    str(r.lhs),
                    str(r.rhs),
                    str(r.confidence),
                    str(r.support),
                    str(r.lift),
                    str(r.conviction)
                ]) + '\n')


data = {}


# Deserialize data produced by a certain step
def deserialize(step):
    if step not in data.keys():
        if os.path.exists(args.o + "step_" + str(step) + ".bin"):
            log(" - Deserializing data from step " + str(step))
            with open(args.o + "step_" + str(step) + ".bin", "rb") as f:
                data[step] = pickle.loads(f.read())
    return data[step]

# ==================================================== #
#                    Data Classes                      #
# ==================================================== #
class PrescType:
    id = ''
    desc = ''

    def __init__(self, id, description):
        self.id = id
        self.desc = description

class PrescGroup:
    id = ''
    desc = ''
    type = None

    def __init__(self, id, description, type):
        self.id = id
        self.desc = description
        self.type = type

class PrescItem:
    id = ''
    desc = ''
    group = None

    def __init__(self, id, description, group):
        self.id = id
        self.desc = description
        self.group = group

class Prescription:
    treatmentId = ''
    # treatmentType = ''
    patientId = ''
    diagnosis = ''
    # sex = ''
    id = ''
    daysBorn = ''
    prescItem = ''
    prescDays = ''

    def __init__(self, id, treatmentId, patientId, diagnosis, daysBorn, prescItem, prescDays):
        self.treatmentId = treatmentId
        self.patientId = patientId
        self.diagnosis = diagnosis
        self.id = id
        self.daysBorn = daysBorn
        self.prescItem = prescItem
        self.prescDays = prescDays


class Transaction:
    treatmentId = ''
    patientId = ''
    prescs = []
    diagnosis = ''
    prescDays = []

    def __init__(self, treatmentId, patientId, prescs, diagnosis, prescDays):
        self.treatmentId = treatmentId
        self.patientId = patientId
        self.diagnosis = diagnosis
        self.prescs = prescs
        self.prescDays = prescDays


# ==================================================== #
#                 Processing Steps                     #
# ==================================================== #

# 0 - Read Data
curStep = 0
if do_run_step(curStep):
    log(" - Opening File")
    dataFrame = pd.read_csv(args.i, delimiter=',', encoding='latin1')

    prescItems = {}
    diagnosis = {}
    prescGroups = {}
    prescTypes = {}

    transactions = {}

    log(" - Processing")
    for index, row in dataFrame.iterrows():
        # Feedback
        if not args.q and index % 10000 == 0:
            log(" - Row: " + str(int(index / 1000)) + "k")

        # Map different objects
        if diagnosis.get(row['CD_CID'], None) is None:
            diagnosis[row['CD_CID']] = row['DS_CID']
        curDiag = diagnosis[row['CD_CID']]

        if prescTypes.get(row['CD_PRE_MED_INDEX'], None) is None:
            prescTypes[row['CD_PRE_MED_INDEX']] = PrescType(id=row['CD_PRE_MED_INDEX'], description=row['NM_OBJETO'])
        if prescGroups.get(row['CD_TIP_ESQ'], None) is None:
            prescGroups[row['CD_TIP_ESQ']] = PrescGroup(id=row['CD_TIP_ESQ'], description=row['DS_TIP_ESQ'], type=prescTypes[row['CD_PRE_MED_INDEX']])
        if prescItems.get(row['CD_TIP_PRESC_INDEX'], None) is None:
            prescItems[row['CD_TIP_PRESC_INDEX']] = PrescItem(row['CD_TIP_PRESC_INDEX'], row['DS_TIP_PRESC'], group=prescGroups[row['CD_TIP_ESQ']])
        curPrescItem = prescItems[row['CD_TIP_PRESC_INDEX']]

        curPresc = Prescription(
            id=row['CD_PRE_MED_INDEX'],
            treatmentId=row['CD_ATENDIMENTO_INDEX'],
            patientId=row['CD_PACIENTE_INDEX'],
            diagnosis=curDiag,
            daysBorn=row['NR_DIAS_NO_ATENDIMENTO'],
            prescItem=curPrescItem,
            prescDays=row['NR_DIAS_PRESCIACAO']
        )

        # group in transactions

        # Transaction = 1 patient + 1 Diagnosis
        # tmp = transactions.get(curPresc.patientId + "___" + curPresc.diagnosis, Transaction(

        # Transaction = one treatment
        tmp=transactions.get(curPresc.treatmentId, Transaction(

        # Transaction = one prescription
        # tmp = transactions.get(curPresc.id, Transaction(
            treatmentId=curPresc.treatmentId,
            patientId=curPresc.patientId,
            prescs=[],
            diagnosis=curPresc.diagnosis,
            prescDays=[]))
        tmp.prescs.append(curPresc.prescItem)
        tmp.prescDays.append(curPresc.prescDays)
        # Transaction = 1 prescription
        # transactions[curPresc.id] = tmp
        # Transaction = 1 patient + 1 Diagnosis
        # transactions[curPresc.patientId + "___" + curPresc.diagnosis] = tmp
        # Transaction = one treatment
        transactions[curPresc.treatmentId] = tmp

    data[curStep] = {
        'Transactions': list(map(lambda kv: kv[1], transactions.items())),
        'PrescItems': prescItems
    }

    # with open("data\\diagnosis.csv", "w") as f:
    #     f.write("code;diagnosis\n")
    #     for key in diagnosis.keys():
    #         diag = diagnosis[key]
    #         f.write(str(key) + ";" + diag + "\n")
    #
    # with open("data\\prescs.csv", "w") as f:
    #     f.write("typeCode;type;groupCode;group;itemCode;item\n")
    #     for key in prescItems.keys():
    #         item = prescItems[key]
    #         f.write(";".join(str(x) for x in [item.id, item.desc, item.group.id, item.group.desc, item.group.type.id, item.group.type.desc]) + "\n")



# 1 - Just organizing
curStep = 1
if do_run_step(curStep):
    data[curStep] = deserialize(0)['Transactions']

# 2 - Remove some groups
curStep = 2
if do_run_step(curStep):
    transactions: [Transaction] = deserialize(1)
    filterGroups = [
        'PROCEDIMENTO AIH', # Prescription for generating document, does not contain relevant information
        'TRANSCRIÇÃO MÉDICA', # Nursing transcription does not contain relevant information
        'SOLICITAÇÃO MATERAIS',  # Used rarely, do not contain relevant information.
        'PRESCRIÇÃO FISIOTERAPIA',
        'PRESCRIÇÃO HEMOTERAPIA',
        'PRESCRIÇÃO FONOAUDIOLOGIA',
        'PRESCRIÇÃO BIOMEDICO',
        'RECEITUÁRIO MÉDICO'
    ]
    data[curStep] = list(map(lambda x: Transaction(treatmentId=x.treatmentId,
                                                   patientId=x.patientId,
                                                   diagnosis=x.diagnosis,
                                                   prescDays=x.prescDays,
                                                   prescs=list(filter(lambda y: y.group.type.desc not in filterGroups, x.prescs))), transactions))

# 3 - Remove empty transactions
curStep = 3
if do_run_step(curStep):
    transactions: [Transaction] = deserialize(2)
    data[curStep] = list(filter(lambda x: len(x.prescs) > 0, transactions))

# 4 - Order prescs by time
curStep = 4
if do_run_step(curStep):
    transactions: [Transaction] = deserialize(3)

    def sorted_items(transaction):
        tmp = [(transaction.prescDays[idx], v) for idx, v in enumerate(transaction.prescs)]
        tmp.sort(key=lambda x: x[0])
        return list(map(lambda x: x[1], tmp))

    data[curStep] = list(map(lambda x: Transaction(treatmentId=x.treatmentId,
                                                   patientId=x.patientId,
                                                   diagnosis=x.diagnosis,
                                                   prescDays=x.prescDays,
                                                   prescs=sorted_items(x)), transactions))

# 5 - Grouping by diagnosis
curStep = 5
if do_run_step(curStep):
    transactions: [Transaction] = deserialize(4)
    data[curStep] = {}

    for t in transactions:
        if t.diagnosis not in data[curStep]:
            data[curStep][t.diagnosis] = list()
        data[curStep][t.diagnosis].append(t)

# TODO -> Maybe remove this
# 15 - Remove transactions with only one item
curStep = 15
if do_run_step(curStep):
    data[curStep] = {}
    goupedTransactions = deserialize(5)

    for diag in goupedTransactions.keys():
        transactions = goupedTransactions[diag]
        data[curStep][diag] = list(filter(lambda x: len(x.prescs) > 1, transactions))

# 16 - Remove diagnosis with less than 100 transactions left (not enough data)
curStep = 16
if do_run_step(curStep):
    goupedTransactions = deserialize(5)
    data[curStep] = {}

    for diag in goupedTransactions.keys():
        transactions = goupedTransactions[diag]
        if len(transactions) > 100:
            data[curStep][diag] = transactions

# 20 - Change to apriori format
curStep = 20
if do_run_step(curStep):
    data[curStep] = {}
    goupedTransactions = deserialize(16)

    def map_func(diag, prescs):
        ret = [diag]
        ret.extend(list(map(lambda x: x.desc, prescs)))
        return ret

    for diag in goupedTransactions.keys():
        transactions = goupedTransactions[diag]
        data[curStep][diag] = list(map(lambda x: list(map(lambda y: y.desc, x.prescs)), transactions))

# 50 - Apriori
curStep = 50
if do_run_step(curStep):
    data[curStep] = {}
    goupedTransactions = deserialize(20)

    # only 5 largest
    g = list(filter(lambda kv: (kv[0], len(kv[1])), goupedTransactions.items()))
    g = sorted(g, key=lambda x: max(x[1]))
    g = g[-5:]
    g = list(map(lambda x: x[0], g))

    diagSpecific = {
        'INFARTO AGUDO TRANSMURAL DA PAREDE INFERIOR DO MIOCARDIO': (0.1, 0.40, 3),
        'INFARTO AGUDO DO MIOCARDIO NAO ESPECIFICADO': (0.1, 0.40, 3),
        '"FLUTTER" E FIBRILACAO ATRIAL': (0.1, 0.40, 3),
        'CIRROSE HEPATICA ALCOOLICA': (0.1, 0.40, 3),
        'ANEMIA NAO ESPECIFICADA': (0.1, 0.40, 3),
        'INFARTO AGUDO TRANSMURAL DA PAREDE ANTERIOR DO MIOCARDIO': (0.1, 0.40, 3),
        'COLECISTITE AGUDA': (0.1, 0.40, 3),
        'ANGINA INSTAVEL': (0.1, 0.40, 3),
        'HEMORRAGIA GASTROINTESTINAL, SEM OUTRA ESPECIFICACAO': (0.1, 0.40, 3),
        'CIRURGIA PROFILATICA NAO ESPECIFICADA': (0.1, 0.40, 3),
        'PNEUMONIA BACTERIANA NAO ESPECIFICADA': (0.1, 0.40, 3),
        'FRATURA DO FEMUR, PARTE NAO ESPECIFICADA': (0.1, 0.40, 3),
        'EDEMA PULMONAR, NAO ESPECIFICADO DE OUTRA FORMA': (0.1, 0.40, 3),
        'OUTRAS COLELITIASES': (0.1, 0.40, 3),
        'INSUFICIENCIA CARDIACA NAO ESPECIFICADA': (0.1, 0.40, 3),
        'NEOPLASIA MALIGNA DO RETO': (0.1, 0.40, 3),
        'NEOPLASIA MALIGNA DA PROSTATA': (0.1, 0.40, 3),
        'RUPTURA PREMATURA DE MEMBRANAS, COM INICIO DO TRABALHO DE PARTO DENTRO DE 24 HORAS': (0.1, 0.40, 3),
        'ARRITMIA CARDIACA NAO ESPECIFICADA': (0.1, 0.40, 3),
        'HEMATEMESE': (0.1, 0.40, 3),
        'TRABALHO DE PARTO PRE-TERMO SEM PARTO': (0.1, 0.40, 3),
        'TRABALHO DE PARTO PRECIPITADO': (0.1, 0.40, 3),
        'ABDOME AGUDO': (0.1, 0.40, 3),
        'BLOQUEIO ATRIOVENTRICULAR TOTAL': (0.1, 0.40, 3),
        'DIABETES MELLITUS INSULINO-DEPENDENTE - COM COMPLICACOES CIRCULATORIAS PERIFERICAS': (0.1, 0.40, 3),
        'OUTRAS PNEUMONIAS BACTERIANAS': (0.1, 0.40, 3),
        'DOENCA PULMONAR OBSTRUTIVA CRONICA COM EXACERBACAO AGUDA NAO ESPECIFICADA': (0.1, 0.40, 3),
        'CALCULOSE DO RIM': (0.1, 0.40, 3),
        'GLAUCOMA NAO ESPECIFICADO': (0.1, 0.40, 3),
        'FRATURA DO CALCANEO': (0.1, 0.40, 3),
        'EXAME DOS OLHOS E DA VISAO': (0.1, 0.40, 3),
        'DISPEPSIA': (0.1, 0.40, 3),
        'EMBOLIA E TROMBOSE VENOSAS DE VEIA NAO ESPECIFICADA': (0.1, 0.40, 3),
        'GRAVIDEZ PROLONGADA': (0.1, 0.40, 3),
        'COMPLICACAO DO PUERPERIO NAO ESPECIFICADA': (0.1, 0.40, 3),
        'CONVALESCENCA APOS CIRURGIA': (0.1, 0.40, 3),
        'FRATURA DA EXTREMIDADE SUPERIOR DO UMERO': (0.1, 0.40, 3),
        'TAQUICARDIA NAO ESPECIFICADA': (0.1, 0.40, 3),
        'OUTROS TIPOS DE PARTO UNICO POR CESARIANA': (0.1, 0.40, 3),
        'INFARTO AGUDO SUBENDOCARDICO DO MIOCARDIO': (0.1, 0.40, 3),
        'PARTO POR CESARIANA DE EMERGENCIA': (0.1, 0.40, 3),
        'NEOPLASIA MALIGNA DA MAMA, NAO ESPECIFICADA': (0.1, 0.40, 3)
    }

    top100Lift = []
    conf1 = []
    top100Conf = []
    top100Conv = []
    i = 0
    for diag in goupedTransactions.keys():
        if diag not in g:
            continue
		
        transactions = goupedTransactions[diag]
        log(' - Running for Diagnosis Group -' + diag + '- (' + str(transactions.__len__()) + ' items)')

        sup=0.05
        conf=0.40
        len=5
        if diag in diagSpecific.keys():
            sup = diagSpecific[diag][0]
            conf = diagSpecific[diag][1]
            len = diagSpecific[diag][2]

        itemSet, rules = apriori(transactions,
                                  min_support=sup,  # Min % of transactions containing item
                                  min_confidence=conf,  # Min chance of B when A
                                  max_length=len)
        log('   * Generated ' + str(rules.__len__()) + ' rules')

        save_apriori_result_csv(curStep, sorted(rules, key=lambda r: r.lift), diag=diag, sufix="_" + str(i))
        i = i+1

        top100Lift.extend(rules)
        top100Lift.sort(key=lambda r: r.lift)
        if top100Lift.__len__() > 100:
            top100Lift = top100Lift[-100:]

        conf1.extend(filter(lambda x: str(x.confidence) == '1', rules))

        top100Conf.extend(filter(lambda x: str(x.confidence) < '0.8', rules))
        top100Conf.sort(key=lambda r: r.confidence)
        if top100Conf.__len__() > 100:
            top100Conf = top100Conf[-100:]

        top100Conv.extend(rules)
        top100Conv.sort(key=lambda r: r.conviction)
        if top100Conv.__len__() > 100:
            top100Conv = top100Conv[-100:]

        save_apriori_result_csv(curStep, sufix="_" + str(i) + '_top100Lift', diag=diag, results=top100Lift)
        save_apriori_result_csv(curStep, sufix="_" + str(i) + '_conf1', diag=diag, results=conf1)
        save_apriori_result_csv(curStep, sufix="_" + str(i) + '_top100Conf', diag=diag, results=top100Conf)
        save_apriori_result_csv(curStep, sufix="_" + str(i) + '_top100Conv', diag=diag, results=top100Conv)
		
        data[curStep][diag] = rules

    save_apriori_result_csv(curStep, data[curStep])
		


# 55 - Isolating results with 100% confidence
# curStep = 55
# if do_run_step(curStep):
#     groupedRules = deserialize(50)
#     data[curStep] = dict(map(lambda kv: (kv[0], list(filter(lambda rule: rule.confidence == 1.0, kv[1]))), groupedRules.items()))
#
#     save_apriori_result_csv(curStep, data[curStep], sufix="conf1")
#
# # 70 - Isolating results with less than 100% confidence
# curStep = 70
# if do_run_step(curStep):
#     data[curStep] = dict(map(lambda kv: (kv[0], list(filter(lambda rule: rule.confidence < 1.0, kv[1]))), groupedRules.items()))
#
#     save_apriori_result_csv(curStep, data[curStep], sufix="confnot1")


# ============ Rerunning but without grouping by diagnosis ===============

# TODO -> Maybe remove this later
# 101 - Remove transactions with only one item
curStep = 101
if do_run_step(curStep):
    transactions = deserialize(4)
    data[curStep] = list(filter(lambda x: len(x.prescs) > 1, transactions))

# 110 - Change to apriori format
curStep = 110
if do_run_step(curStep):
    transactions = deserialize(101)

    def map_func(diag, prescs):
        ret = [diag]
        ret.extend(list(map(lambda x: x.desc, prescs)))
        return ret

    data[curStep] = list(map(lambda x: list(map(lambda y: y.desc, x.prescs)), transactions))

# 150 - Apriori
curStep = 150
if do_run_step(curStep):
    transactions = deserialize(110)
    itemsets, rules = apriori(transactions,
                              min_confidence=0.5,  # Min chance of B when A
                              min_support=0.1,  # Min % of transactions containing item
                              max_length=2)

    data[curStep] = sorted(rules, key=lambda r: r.lift)

    save_apriori_result_csv(curStep, rules)

# Serialize Data
print("")

for stepNum in data.keys():
    if do_run_step(stepNum, quiet=True):
        stepData = data[stepNum]
        print("Serializing data from step " + str(stepNum))
        serialized = pickle.dumps(stepData)
        with open(args.o + "step_" + str(stepNum) + ".bin", "wb") as f:
            f.write(serialized)
        if args.json:
            try:
                serialized = json.dumps(stepData)
                with open(args.o + "step_" + str(stepNum) + ".json", "w") as f:
                    f.write(serialized)
            except:
                print(" - Error while serializing json in step " + str(stepNum))

