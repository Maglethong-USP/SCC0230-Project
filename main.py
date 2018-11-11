# Load libraries
import typing
from efficient_apriori import apriori
import argparse
import xlrd
import pandas as pd
import pickle
import json
import argparse
import os

# Command line Arguments
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
args = parser.parse_args()

if args.interact:
    args.only_steps = input('Enter steps to run [may enter multiple separated by comma]: ')

if not (os.path.exists(args.o)) or not (os.path.isdir(args.o)):
    os.mkdir(args.o)

if args.o != '' and not (args.o.endswith('\\')):
    args.o = args.o + "\\"


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
            print('Running step ' + str(step))
        else:
            print('Skipping step ' + str(step))

    return run


def save_csv_dict(step, dic_of_list, sufix=""):
    with open(args.o + "step_" + str(step) + sufix + ".csv", "w") as f:
        for key in dic_of_list.keys():
            f.write(str(key) + ';' + ";".join(str(x) for x in dic_of_list[key]) + '\n')


def save_csv(step, list_of_list, sufix=""):
    with open(args.o + "step_" + str(step) + sufix + ".csv", "w") as f:
        for list in list_of_list:
            f.write(";".join(str(x) for x in list) + '\n')


# Deserialize data
data = {}


def deserialize(step):
    if step not in data.keys():
        if os.path.exists(args.o + "step_" + str(step) + ".bin"):
            print(" - Deserializing data from step " + str(step))
            with open(args.o + "step_" + str(step) + ".bin", "rb") as f:
                data[step] = pickle.loads(f.read())
    return data[step]


# 0 - Pre-processing data
curStep = 0
if do_run_step(curStep):
    print(" - Opening File")
    dataFrame = pd.read_csv(args.i, delimiter=';', encoding='latin1')

    atendimentos = {}
    pacientes = set()
    hipDiag = {}
    tiposPresc = {}
    grupos = {}
    prescricoes = {}
    transactions = {}
    types = {}

    print(" - Processing")
    for index, row in dataFrame.iterrows():
        if index % 1000 == 0:
            if index % 10000 == 0:
                print(" - Row: " + str(int(index / 1000)) + "k")
            else:
                print(".")

        # Fetching enumerations and patient ids
        pacientes.add(row['CD_PACIENTE_INDEX'])
        hipDiag[row['CD_CID']] = row['DS_CID']
        tiposPresc[row['CD_PRE_MED_INDEX']] = row['NM_OBJETO']
        grupos[row['CD_TIP_ESQ']] = row['DS_TIP_ESQ']
        prescricoes[row['CD_TIP_PRESC_INDEX']] = row['DS_TIP_PRESC']
        # Record transactions per treatmentId
        key = row['CD_ATENDIMENTO_INDEX']
        tmp = transactions.get(key, list())
        tmp.append(row['CD_TIP_PRESC_INDEX'])
        transactions[key] = tmp
        atendimentos[key] = [row['CD_PACIENTE_INDEX'], row['CD_CID']]

        # check missing data
        if row['CD_PRE_MED_INDEX'] == '':
            print("no type!")
        if row['CD_TIP_ESQ'] == '':
            print("no group!")
        if row['CD_TIP_PRESC_INDEX'] == '':
            print("no presc!")

        # check inconsistent data
        idKey = 'CD_CID'
        valueKey = 'DS_CID'
        if hipDiag[row[idKey]] != '' and hipDiag[row[idKey]] != row[valueKey]:
            print("Inconsistent Diagnostics Code: Previous: " + str(row[idKey]) + " -> " + hipDiag[
                row[idKey]] + " | New: " + str(row[idKey]) + " -> " + row[valueKey])

        idKey = 'CD_PRE_MED_INDEX'
        valueKey = 'NM_OBJETO'
        if tiposPresc[row[idKey]] != '' and tiposPresc[row[idKey]] != row[valueKey]:
            print("Inconsistent Type Code: Previous: " + str(row[idKey]) + " -> " + tiposPresc[
                row[idKey]] + " | New: " + str(row[idKey]) + " -> " + row[valueKey])

        idKey = 'CD_TIP_ESQ'
        valueKey = 'DS_TIP_ESQ'
        if grupos[row[idKey]] != '' and grupos[row[idKey]] != row[valueKey]:
            print("Inconsistent Group Code: Previous: " + str(row[idKey]) + " -> " + grupos[
                row[idKey]] + " | New: " + str(row[idKey]) + " -> " + row[valueKey])

        idKey = 'CD_TIP_PRESC_INDEX'
        valueKey = 'DS_TIP_PRESC'
        if prescricoes[row[idKey]] != '' and prescricoes[row[idKey]] != row[valueKey]:
            print("Inconsistent Prescription Code: Previous: " + str(row[idKey]) + " -> " + prescricoes[
                row[idKey]] + " | New: " + str(row[idKey]) + " -> " + row[valueKey])

        # Construct object
        typeCode = row['CD_PRE_MED_INDEX']
        if types.get(typeCode, '') == '':
            types[typeCode] = {
                'Code': typeCode,
                'Description': row['NM_OBJETO'],
                'Groups': {}
            }
        groupCode = row['CD_TIP_ESQ']
        if types[typeCode]['Groups'].get(groupCode, '') == '':
            types[typeCode]['Groups'][groupCode] = {
                'Code': typeCode,
                'Description': row['DS_TIP_ESQ'],
                'Prescriptions': {}
            }
        prescCode = row['CD_TIP_PRESC_INDEX']
        if types[typeCode]['Groups'][groupCode]['Prescriptions'].get(prescCode, '') == '':
            types[typeCode]['Groups'][groupCode]['Prescriptions'][prescCode] = {
                'Code': typeCode,
                'Description': row['DS_TIP_PRESC'],
            }

    data[curStep] = {
        'Treatments': atendimentos,
        'Patients': list(pacientes),
        'Diagnosis': hipDiag,
        'Transactions': transactions,
        'PrescriptionTypes': types,
        'Types': tiposPresc,
        'Groups': grupos,
        'Prescs': prescricoes
    }

    save_csv_dict(curStep, transactions)

# TODO -> Remove grousp in ['PROCEDIMENTO AIH', 'TRANSCRIÇÃO MÉDICA']

# 5 - Grouping by diagnosis
curStep = 5
if do_run_step(curStep):
    data[curStep] = {}
    transactions = deserialize(0)['Transactions']
    treatments = deserialize(0)['Treatments']
    for key in transactions.keys():
        diag = treatments[key][1]
        tmp = data[curStep].get(diag, list())
        tmp.append(transactions[key])
        data[curStep][diag] = tmp

    # Save csv
    with open(args.o + "step_" + str(curStep) + ".csv", "w") as f:
        f.write('Diagnosis; Transaction\n')
        for diag in data[curStep].keys():
            for transaction in data[curStep][diag]:
                f.write(diag + ";" + ";".join(str(x) for x in transaction) + '\n')

# 15 - Remove single transactions (cutting boring ones ~8k -> ~2k)
curStep = 15
if do_run_step(curStep):
    data[curStep] = {}
    groups = deserialize(5)
    for groupKey in groups.keys():
        transactions = groups[groupKey]
        data[curStep][groupKey] = []
        for transaction in transactions:
            if transaction.__len__() > 1:
                data[curStep][groupKey].append(transaction)

    # Save csv
    with open(args.o + "step_" + str(curStep) + ".csv", "w") as f:
        f.write('Diagnosis; Transaction\n')
        for diag in data[curStep].keys():
            for transaction in data[curStep][diag]:
                f.write(diag + ";" + ";".join(str(x) for x in transaction) + '\n')

# 16 - Remove diagnosis with less than 50 transactions left
curStep = 16
if do_run_step(curStep):
    data[curStep] = {}
    groups = deserialize(15)
    for groupKey in groups.keys():
        transactions = groups[groupKey]
        if transactions.__len__() >= 50:
            data[curStep][groupKey] = transactions

    # Save csv
    with open(args.o + "step_" + str(curStep) + ".csv", "w") as f:
        f.write('Diagnosis; Transaction\n')
        for diag in data[curStep].keys():
            for transaction in data[curStep][diag]:
                f.write(diag + ";" + ";".join(str(x) for x in transaction) + '\n')

# 20 - Decode Prescription and Diagnosis IDs
curStep = 20
if do_run_step(curStep):
    data[curStep] = {}
    groups = deserialize(16)
    deserialize(0)
    for groupKey in groups.keys():
        transactions = groups[groupKey]
        tmp = []
        for transaction in transactions:
            tmp.append(list(map(lambda x: data[0]['Prescs'].get(x, '[???]'), transaction)))
        data[curStep][data[0]['Diagnosis'].get(groupKey, '[???]')] = tmp

    # Save csv
    with open(args.o + "step_" + str(curStep) + ".csv", "w") as f:
        f.write('Diagnosis; Transaction\n')
        for diag in data[curStep].keys():
            for transaction in data[curStep][diag]:
                f.write(diag + ";" + ";".join(transaction) + '\n')

# 50 - Apriori
curStep = 50
if do_run_step(curStep):
    data[curStep] = {}
    deserialize(20)
    for diagnosis in data[20].keys():
        transactions = data[20][diagnosis]
        # if diagnosis == 'DOENCA ATEROSCLEROTICA DO CORACAO': # Bugging
        #     print(" - skipping -" + diagnosis + "-")
        #     continue
        print(' - Running for Diagnosis Group -' + diagnosis + '- (' + str(transactions.__len__()) + ' items)')
        itemsets, rules = apriori(transactions,
                                  min_confidence=0.5,  # Min chance of B when A
                                  min_support=0.5,  # Min % of transactions containing item
                                  max_length=2)

        data[curStep][diagnosis] = {
            'ItemSets': itemsets,
            'Rules': sorted(rules, key=lambda r: r.confidence)
        }

# 51 - Converting to readable format
curStep = 51
if do_run_step(curStep):
    results = deserialize(50)
    data[curStep] = []

    for diag in results.keys():
        for rule in results[diag]['Rules']:
            data[curStep].append([diag, str(rule.lhs), str(rule.rhs), str(rule.confidence), str(rule.support),
                                 str(rule.lift), str(rule.conviction)])

    save = [['Diagnosis', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']]
    save.extend(data[curStep])
    save_csv(curStep, save)

# 55 - Isolating results with 100% confidence
curStep = 55
if do_run_step(curStep):
    data[curStep] = list(filter(lambda x: float(x[3]) == 1.0, deserialize(51)))

    save = [['Diagnosis', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']]
    save.extend(data[curStep])
    save_csv(curStep, save)

# 56 - 100% confidence - grouping mutual existing
curStep = 56
if do_run_step(curStep):
    data[curStep] = list(map(lambda x: x, deserialize(55)))
    for index, item in enumerate(data[curStep]):
        tmp = list(filter(lambda x: x[1] == item[2] and x[2] == item[1], data[curStep])).__len__()
        data[curStep][index].append(tmp > 0)

    data[curStep] = sorted(data[curStep], key=lambda x: x[7])

    save = [['Diagnosis', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv', 'Mutual']]
    save.extend(data[curStep])
    save_csv(curStep, save)

# 70 - Isolating results with less than 100% confidence
curStep = 70
if do_run_step(curStep):
    data[curStep] = list(filter(lambda x: float(x[3]) < 1.0, deserialize(51)))

    save = [['Diagnosis', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']]
    save.extend(data[curStep])
    save_csv(curStep, save)

# 71 - Removing rules already present when not grouping by diagnosis
curStep = 71
if do_run_step(curStep):
    globalRules = deserialize(104)['Rules']
    tmp = deserialize(70).__len__()
    data[curStep] = list(filter(lambda x: len(list(filter(lambda y: str(y.lhs) == str(x[1]) and str(y.rhs) == str(x[2]), globalRules))) == 0, deserialize(70)))
    print(" - Filtered " + str(tmp - data[curStep].__len__()) + " rules of " + str(tmp) + ". " + str(data[curStep].__len__()) + " left")

    save = [['Diagnosis', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']]
    save.extend(data[curStep])
    save_csv(curStep, save)

# 72 - Adding prescription group to data
curStep = 72
if do_run_step(curStep):
    runFor = [71, 56, 105]

    def map_presc_group():
        types = deserialize(0)['PrescriptionTypes']
        prescs = {}
        for type in types.values():
            for group in type['Groups'].values():
                for presc in group['Prescriptions'].values():
                    prescs[presc['Description']] = group['Description']
        return prescs

    map_pg = map_presc_group()
    for step in runFor:
        curData = []
        try:
            curData = deserialize(step)
        except:
            print(" - Couldnt process for step " + str(step))
            continue

        for d in curData:
            for i in [1, 2]:
                d[i] = str(d[i])[2:-3]
                group = map_pg.get(d[i], '[???]')
                d[i] = d[i] + " (" + group + ")"

        save = [['Diagnosis', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']]
        save.extend(curData)
        save_csv(step, save, "_group")

# test_1 - testing only
curStep = 'test_1'
if do_run_step(curStep):
    curData = deserialize(71)


# ============ Rerunning but without grouping by diagnosis ===============

# 101 - Change to list of list
curStep = 101
if do_run_step(curStep):
    data[curStep] = []
    deserialize(0)
    transactions = data[0]['Transactions']
    for key in transactions.keys():
        data[curStep].append(transactions[key])

    save_csv(curStep, list(map(lambda x: list(map(lambda y: data[0]['Prescs'].get(y, '[???]'), x)), data[curStep])))

# 102 - Remove single transactions (cutting boring ones ~8k -> ~2k)
curStep = 102
if do_run_step(curStep):
    deserialize(101)
    data[curStep] = list(filter(lambda x: len(x) > 1, data[101]))

    save_csv(curStep, list(map(lambda x: list(map(lambda y: data[0]['Prescs'].get(y, '[???]'), x)), data[curStep])))

# 103 - [Testing] Decode Prescription IDs
curStep = 103
if do_run_step(curStep):
    deserialize(102)
    data[curStep] = list(map(lambda x: list(map(lambda y: data[0]['Prescs'].get(y, '[???]'), x)), data[102]))

# 104 - Apriori
curStep = 104
if do_run_step(curStep):
    deserialize(103)
    itemsets, rules = apriori(data[103],
                              min_confidence=0.5,  # Min chance of B when A
                              min_support=0.1,  # Min % of transactions containing item
                              max_length=2)

    data[curStep] = {
        'ItemSets': itemsets,
        'Rules': sorted(rules, key=lambda r: r.lift)
    }

# 105 - Converting to readable format
curStep = 105
if do_run_step(curStep):
    results = deserialize(104)
    data[curStep] = list(map(lambda r: ['', r.lhs, r.rhs, r.confidence, r.support, r.lift, r.conviction], results['Rules']))

    save = [['', 'LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']]
    save.extend(data[curStep])
    save_csv(curStep, save)

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
