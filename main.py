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
                    default=False)
parser.add_argument('-step',
                    help='First step to run',
                    default=0)
parser.add_argument('-only-steps',
                    help='Runs specified steps only. NOTE: The run order will always be crescent step number.',
                    default='')
parser.add_argument('-i',
                    help='Run interactive: ask for whats steps to run',
                    default=False)
parser.add_argument('-json',
                    help='Serialize using json format?',
                    default=True)
args = parser.parse_args()

if args.i:
    args.only_steps = input('Enter steps to run [may enter multiple separated by comma]: ')


def do_run_step(step):
    run = False
    if args.only_steps == '':
        if bool(args.all) and args.step <= curStep:
            run = True
        elif args.step == curStep:
            run = True
    else:
        if args.only_steps.split(',').__contains__(str(step)):
            run = True

    if run:
        print('Running step ' + str(step))
    else:
        print('Skipping step ' + str(step))

    return run


def save_csv_dict(step, dic_of_list, sufix=""):
    with open("data\step_" + str(step) + sufix + ".csv", "w") as f:
        for key in dic_of_list.keys():
            f.write(key + ';' + ";".join(str(x) for x in dic_of_list[key]) + '\n')


def save_csv(step, list_of_list, sufix=""):
    with open("data\step_" + str(step) + sufix + ".csv", "w") as f:
        for list in list_of_list:
            f.write(";".join(str(x) for x in list) + '\n')


# Deserialize data
print("")
data = {}
if os.path.exists("data") and os.path.isdir("data"):
    i = 0
    while os.path.exists("data\step_" + str(i) + ".bin"):
        print("Deserializing data from step " + str(i))
        with open("data\step_" + str(i) + ".bin", "rb") as f:
            serialized = f.read()
        data[i] = pickle.loads(serialized)
        i = i + 1

# Definitions
print("")
# TODO -> consts to tablecodeString

# 0 - Pre-processing data
curStep = 0
if do_run_step(curStep):
    print(" - Opening File")
    dataFrame = pd.read_excel("data-sm.xlsx")

    pacientes = set()
    hipDiag = {}
    tiposPresc = {}
    grupos = {}
    prescricoes = {}
    transactions = {}
    types = {}

    print(" - Processing")
    for index, row in dataFrame.iterrows():
        # Fetching enumerations and patient ids
        pacientes.add(row['CD_PACIENTE_INDEX'])
        hipDiag[row['CD_CID']] = row['DS_CID']
        tiposPresc[row['CD_PRE_MED_INDEX']] = row['NM_OBJETO']
        grupos[row['CD_TIP_ESQ']] = row['DS_TIP_ESQ']
        prescricoes[row['CD_TIP_PRESC_INDEX']] = row['DS_TIP_PRESC']
        # Record transactions per patient / per diagnosis
        key = str(row['CD_PACIENTE_INDEX']) + "___" + str(row['CD_CID'])
        tmp = transactions.get(key, list())
        tmp.append(row['CD_TIP_PRESC_INDEX'])
        transactions[key] = tmp

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
        'Patients': list(pacientes),
        'Diagnosis': hipDiag,
        'Transactions': transactions,
        'PrescriptionTypes': types,
        'Types': tiposPresc,
        'Groups': grupos,
        'Prescs': prescricoes
    }

    save_csv_dict(curStep, transactions)

# 1 - Change to list of list
curStep = 1
if do_run_step(curStep):
    data[curStep] = []
    transactions = data[curStep - 1]['Transactions']
    for key in transactions.keys():
        data[curStep].append(transactions[key])

    save_csv(curStep, list(map(lambda x: list(map(lambda y: data[0]['Prescs'].get(y, '[???]'), x)), data[curStep])))

# 2 - Remove single transactions (cutting boring ones ~8k -> ~2k)
curStep = 2
if do_run_step(curStep):
    data[curStep] = list(filter(lambda x: len(x) > 1, data[curStep - 1]))

    save_csv(curStep, list(map(lambda x: list(map(lambda y: data[0]['Prescs'].get(y, '[???]'), x)), data[curStep])))

# 3 - [Testing] Decode Prescription IDs
curStep = 3
if do_run_step(curStep):
    data[curStep] = list(map(lambda x: list(map(lambda y: data[0]['Prescs'].get(y, '[???]'), x)), data[curStep - 1]))

# 4 - Apriori
curStep = 4
if do_run_step(curStep):
    itemsets, rules = apriori(data[curStep - 1],
                              min_confidence=0.5,  # Min chance of B when A
                              min_support=0.1)  # Min % of transactions containing item

    data[curStep] = {
        'ItemSets': itemsets,
        'Rules': sorted(rules, key=lambda r: r.lift)
    }

    lines = [['LHS', 'RHS', 'Conf', 'Supp', 'Lift', 'Conv']]
    lines.extend(map(lambda r: [r.lhs, r.rhs, r.confidence, r.support, r.lift, r.conviction], data[curStep]['Rules']))
    save_csv(curStep, lines)

# Serialize Data
print("")
if not (os.path.exists("data")) or not (os.path.isdir("data")):
    os.mkdir('data')

for stepNum in data.keys():
    stepData = data[stepNum]
    print("Serializing data from step " + str(stepNum))
    serialized = pickle.dumps(stepData)
    with open("data\step_" + str(stepNum) + ".bin", "wb") as f:
        f.write(serialized)
    if args.json:
        try:
            serialized = json.dumps(stepData)
            with open("data\step_" + str(stepNum) + ".json", "w") as f:
                f.write(serialized)
        except:
            print(" - Error while serializing json in step " + str(stepNum))

# Next Steps:
# - Group by diagnosis
