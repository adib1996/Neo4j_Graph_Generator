import os
import pandas as pd
import time

CURRENT_FOLDER = os.path.dirname('__file__')
DATA_FOLDER = os.path.join(CURRENT_FOLDER, 'data')
RAW_DATA_FOLDER = os.path.join(CURRENT_FOLDER, 'raw_input')
TRANSFORMED_DATA_FILE_NAME = 'data.csv'
TRANSFORMED_DATA_FILE_PATH = os.path.join(DATA_FOLDER, TRANSFORMED_DATA_FILE_NAME)
RAW_DATA_FILE_NAME_LIST = ['infectious_diseases.xlsx']
# RAW_DATA_FILE_NAME_LIST = ['test.xlsx']
DISEASE_LIST_FILE_NAME = 'diseaseList.xlsx'
REQUIRED_DATA_COLUMNS = ["Start Node Label", "Start Node Attribute Name", "Start Node Attribute Value",
                         "Graph Element", "Attribute Name", "Attribute Value", "Relation Name",
                         "Relation Attribute Name", "Relation Attribute Value",
                         "End Node Label", "End Node Attribute Name", "End Node Attribute Value", "Record Tag"]
REL_REQUIRED_COLS = ["Start Node Label", "Start Node Attribute Name",
                     "Start Node Attribute Value", "Graph Element", "Relation Name",
                     "End Node Label", "End Node Attribute Name", "End Node Attribute Value",
                     "Record Tag"]

ATTR_REQUIRED_COLS = ["Start Node Label", "Start Node Attribute Name", "Start Node Attribute Value",
                      "Graph Element", "Attribute Name", "Attribute Value", "Record Tag"]


def get_disease_list():
    disease_list_data = pd.read_excel(os.path.join(RAW_DATA_FOLDER, DISEASE_LIST_FILE_NAME), header=None)
    disease_list_data.columns = ['name']
    disease_list_data = disease_list_data.loc[((disease_list_data['name'] != "") &
                                               (disease_list_data['name'].isnull() == False)), :].reset_index(drop=True)
    disease_list = list(disease_list_data['name'].values)
    print('\nNumber of diseases: %d' % len(disease_list))
    return disease_list


def get_empty_data_dict():
    data = {}
    for col in REQUIRED_DATA_COLUMNS:
        data[col] = []
    return data


def get_disease_block_range_using_names(sheet_data):
    disease_block_range = []
    for j in range(len(sheet_data)):
        if sheet_data['Info(DSHE)'].iloc[j] in disease_list:
            if len(disease_block_range) == 0:
                disease_block_range.append([sheet_data['Info(DSHE)'].iloc[j], j])
            else:
                disease_block_range[len(disease_block_range) - 1].append(j - 1)
                disease_block_range.append([sheet_data['Info(DSHE)'].iloc[j], j])
    if len(disease_block_range) > 0:
        disease_block_range[len(disease_block_range) - 1].append(len(sheet_data))
    return disease_block_range


def get_disease_block_range_using_data_structure(sheet_data):
    disease_block_range = []
    block_start = 0
    j = 0
    while j < len(sheet_data):
        if sheet_data['Info(DSHE)'].iloc[j] == '':
            block_end = j
            disease_block_range.append([sheet_data['Info(DSHE)'].iloc[block_start], block_start, block_end - 1])
            # loop further to increment j to escape blanks
            while sheet_data['Info(DSHE)'].iloc[j] == '':
                j += 1
            block_start = j
        else:
            j += 1
    block_end = len(sheet_data) + 1
    disease_block_range.append([sheet_data['Info(DSHE)'].iloc[block_start], block_start, block_end - 1])
    return disease_block_range


def get_meta_tags_by_sheet(sheet_data, disease_block_range):
    meta_tags = []
    for disease_block in disease_block_range:
        subset = sheet_data.loc[disease_block[1] + 1:disease_block[2], :]
        for r in range(len(subset)):
            if subset['Info(DSHE)'].iloc[r].strip() not in meta_tags:
                meta_tags.append(subset['Info(DSHE)'].iloc[r].strip())
    return meta_tags


def get_meta_tags_by_text(text):
    sheet_data_col_names = ['Info(DSHE)', 'Value', 'Synonym', 'Attribute', 'Attribute Category']
    result_tags = []
    for file_name in RAW_DATA_FILE_NAME_LIST:
        # print(file_name)
        xlsx = pd.ExcelFile(os.path.join(RAW_DATA_FOLDER, file_name))
        sheet_names = xlsx.sheet_names
        for i in range(len(sheet_names)):
            # print('\n\nSheet name: %s\n' % sheet_names[i])
            sheet_data = xlsx.parse(sheet_names[i], header=None)
            if sheet_data.shape[1] > 5:
                sheet_data = sheet_data.iloc[:, 0:5]
            if sheet_data.shape[1] < 5:
                for col in sheet_data_col_names[sheet_data.shape[1]:5]:
                    sheet_data.loc[:, col] = ""
            sheet_data.columns = sheet_data_col_names
            sheet_data = sheet_data.fillna('')
            disease_block_range = get_disease_block_range_using_names(sheet_data)
            meta_tags_by_sheet = get_meta_tags_by_sheet(sheet_data, disease_block_range)
            result_tags += [x for x in meta_tags_by_sheet if text.lower() in x.lower()]
    return list(set(result_tags))


def get_resolved_disease_name(name):
    resolved_name = name.strip()
    if '.' in name:
        if name.index('.') < 3:
            resolved_name = name.split('.')[1].strip()
    return resolved_name.lower()


def get_clean_text(text):
    clean_text = text.strip()
    if len(text.strip()) > 0:
        if ',' in text:
            if text[-1] == ',':
                clean_text = text[:-1].strip()
            if clean_text[0] == ',':
                clean_text = clean_text[1:].strip()
    return clean_text


def get_transformed_data(data):
    # Reading the Raw XLSX file
    sheet_data_col_names = ['Info(DSHE)', 'Value', 'Synonym', 'Attribute', 'Attribute Category']
    for ctr in range(len(RAW_DATA_FILE_NAME_LIST)):
        xlsx = pd.ExcelFile(os.path.join(RAW_DATA_FOLDER, RAW_DATA_FILE_NAME_LIST[ctr]))
        sheet_names = xlsx.sheet_names
        for i in range(len(sheet_names)):
            # print('\n\nSheet name: %s\n' % sheet_names[i])
            sheet_data = xlsx.parse(sheet_names[i], header=None)
            if sheet_data.shape[1] > 5:
                sheet_data = sheet_data.iloc[:, 0:5]
            if sheet_data.shape[1] < 5:
                for col in sheet_data_col_names[sheet_data.shape[1]:5]:
                    sheet_data.loc[:, col] = ""
            sheet_data.columns = sheet_data_col_names
            sheet_data = sheet_data.fillna('')
            # Get disease block range
            disease_block_range = get_disease_block_range_using_names(sheet_data)
            # Get meta tags
            # meta_tags = get_meta_tags_by_sheet(sheet_data, disease_block_range)
            # Possible symptoms tags
            symptom_tags = ['symtpom', 'symtom', 'symptom', 'symptoms', 'symptom/general physical examination',
                            'symmptom', 'symmptom/GPE', 'sympton', 'SYMPTOM', 'SYmptom', 'Symptoms', 'Symptom',
                            'sympyom', 'general physical examination']
            for disease_block in disease_block_range:
                disease_name = get_resolved_disease_name(disease_block[0])
                subset = sheet_data.loc[disease_block[1] + 1:disease_block[2], :]
                # Disease - Symptom Relationship
                rel_other_cols = list(set(REQUIRED_DATA_COLUMNS) - set(REL_REQUIRED_COLS))
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in symptom_tags:
                        if subset['Value'].iloc[r].strip().lower() != "":
                            symptom_name = get_clean_text(subset['Value'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Disease')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(disease_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('hasSymptom')
                            data["End Node Label"].append('Symptom')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(symptom_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")

                # Symptom - Attribute Relationship
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in symptom_tags:
                        if (subset['Value'].iloc[r].strip().lower() != "") & (
                                subset['Attribute'].iloc[r].strip() != ""):
                            symptom_name = get_clean_text(subset['Value'].iloc[r].strip().lower())
                            attribute_name = get_clean_text(subset['Attribute'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Symptom')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(symptom_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('hasAttribute')
                            data["End Node Label"].append('Attribute')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(attribute_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")
                # Symptom - Synonym Attribute
                rel_some_other_cols = list(set(REQUIRED_DATA_COLUMNS) - set(ATTR_REQUIRED_COLS))
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in symptom_tags:
                        if (subset['Value'].iloc[r].strip().lower() != "") & (
                                subset['Synonym'].iloc[r].strip() != ""):
                            symptom_name = get_clean_text(subset['Value'].iloc[r].strip().lower())
                            synonym_name = get_clean_text(subset['Synonym'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Symptom')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(symptom_name)
                            data["Graph Element"].append('Attribute')
                            data["Attribute Name"].append('synonym')
                            data["Attribute Value"].append(synonym_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_some_other_cols:
                                data[col].append("")
                # Disease - Synonym Attribute
                # for disease_block in disease_block_range:
                #     disease_name = get_resolved_disease_name(disease_block[0])
                #     subset = sheet_data.loc[disease_block[1] + 1:disease_block[2], :]
                # rel_some_other_cols = list(set(REQUIRED_DATA_COLUMNS) - set(ATTR_REQUIRED_COLS))
                # for r in range(len(subset)):
                #         if (subset['Value'].iloc[r].strip().lower() != "") & (
                #                 subset['Synonym'].iloc[r].strip() != ""):
                #             synonym_name = get_clean_text(subset['Synonym'].iloc[r].strip().lower())
                #             data["Start Node Label"].append('Disease')
                #             data["Start Node Attribute Name"].append('name')
                #             data["Start Node Attribute Value"].append(disease_name)
                #             data["Graph Element"].append('Attribute')
                #             data["Attribute Name"].append('synonym')
                #             data["Attribute Value"].append(synonym_name)
                #             data["Record Tag"].append(disease_name)
                #             for col in rel_some_other_cols:
                #                 data[col].append("")

                # Attibute - Disease Relationship
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in symptom_tags:
                        if subset['Attribute'].iloc[r].strip() != "":
                            attribute_name = get_clean_text(
                                subset['Attribute'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Attribute')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(attribute_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('associatedWith')
                            data["End Node Label"].append('Disease')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(disease_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")
                # Attibute - Attribute Category Relationship
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in symptom_tags:
                        if (subset['Attribute'].iloc[r].strip() != "") & (
                                subset['Attribute Category'].iloc[r].strip() != ""):
                            attribute_name = get_clean_text(subset['Attribute'].iloc[r].strip().lower())
                            attribute_category_name = get_clean_text(
                                subset['Attribute Category'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Attribute')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(attribute_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('hasCategory')
                            data["End Node Label"].append('AttributeCategory')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(attribute_category_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")
                # Disease - Past History linkage
                past_history_tags = ['Past History', 'past hiatory', 'past history', 'past History', 'Past istory',
                                     'Past history', 'PAst History', 'past  history', 'Past  History',
                                     'PAst history', 'past hsitory']
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in past_history_tags:
                        if subset['Value'].iloc[r].strip() != "":
                            past_history_name = get_clean_text(subset['Value'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Disease')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(disease_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('historyOf')
                            data["End Node Label"].append('PastHistory')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(past_history_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")
                # Disease - Drug History linkage
                drug_history_tags = ['Drug hsitory', 'Drug  history', 'drug history', 'drug hsitory', 'Drugs history',
                                     'Drugh history', 'DRUG HISTORY', 'Drug History', 'Drug history']
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in drug_history_tags:
                        if subset['Value'].iloc[r].strip() != "":
                            drug_history_name = get_clean_text(subset['Value'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Disease')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(disease_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('historyOf')
                            data["End Node Label"].append('DrugHistory')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(drug_history_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")
                # Disease - Family History linkage
                family_history_tags = ['family history', 'family  history', 'Family History',
                                       'family History', 'Family history']
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in family_history_tags:
                        if subset['Value'].iloc[r].strip() != "":
                            family_history_name = get_clean_text(subset['Value'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Disease')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(disease_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('historyOf')
                            data["End Node Label"].append('FamilyHistory')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(family_history_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")
                # Disease - Personal and Social History linkage
                ps_history_tags = ['Physical and social history', 'Personal or social history',
                                   'personal and Social history', 'Social history', 'Personal andr Social history',
                                   'Personal and  social history', 'Personal and social hsitory',
                                   'Persocal and social history', 'Personal and Social History',
                                   'Personal and social examination', 'personal and social history',
                                   'Personal and social  history', 'Personal and social history',
                                   'Personal & Social history', 'Persona and social history',
                                   'Personal & social history', 'Personal social history',
                                   'personal and social History', 'Perosnal and social history',
                                   'Personal and Social history', 'personal and Social History', 'social history',
                                   'Personal and social hisrtory', 'Personal and social History',
                                   'personal and social  history', 'persoanl and social history', 'Birth History']
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in ps_history_tags:
                        if subset['Value'].iloc[r].strip() != "":
                            ps_history_name = get_clean_text(subset['Value'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Disease')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(disease_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('historyOf')
                            data["End Node Label"].append('PersonalAndSocialHistory')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(ps_history_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")  # Disease - Examination linkage
                # Disease - Examination linkage
                examination_tags = ['examination of thyroid galnd', 'sign / examination', 'Examination',
                                    'symptom/general physical examination', 'abdominal examination',
                                    'Respiratory examination', 'general physical examination', 'examination',
                                    'skin examination', 'Personal and social examination', 'oral examination']
                for r in range(len(subset)):
                    if subset['Info(DSHE)'].iloc[r].strip() in examination_tags:
                        if subset['Value'].iloc[r].strip() != "":
                            examination_name = get_clean_text(subset['Value'].iloc[r].strip().lower())
                            data["Start Node Label"].append('Disease')
                            data["Start Node Attribute Name"].append('name')
                            data["Start Node Attribute Value"].append(disease_name)
                            data["Graph Element"].append('Relation')
                            data["Relation Name"].append('hasExamination')
                            data["End Node Label"].append('Examination')
                            data["End Node Attribute Name"].append('name')
                            data["End Node Attribute Value"].append(examination_name)
                            data["Record Tag"].append(disease_name)
                            for col in rel_other_cols:
                                data[col].append("")
    #  Convert the data to a PDF
    data = pd.DataFrame(data)
    data = data[REQUIRED_DATA_COLUMNS]
    return data


if __name__ == "__main__":
    t0 = time.time()
    disease_list = get_disease_list()
    data = get_empty_data_dict()
    data = get_transformed_data(data)

    # Generate Synonyms for symptoms
    data1 = data.loc[data['Start Node Label'] == 'Symptom']
    data1 = data.loc[data['Graph Element'] == 'Attribute']
    df2 = data.loc[data['Graph Element'] != 'Attribute']
    df_temp = data1.groupby(
        ['Start Node Label', 'Start Node Attribute Name', 'Graph Element', 'Attribute Name',
         'Start Node Attribute Value'])['Attribute Value'].unique()
    df_tem_2 = data1.groupby(['Record Tag', 'Start Node Label', 'Start Node Attribute Name', 'Graph Element', 'Attribute Name',
                   'Start Node Attribute Value'])['Attribute Value'].unique()
    df_req1 = pd.DataFrame(df_temp)
    df_req2 = pd.DataFrame(df_tem_2)
    df_req1 = df_req1.reset_index()
    df_req2 = df_req2.filter(
        ['Record Tag', 'Start Node Label', 'Start Node Attribute Name', 'Graph Element', 'Attribute Name',
         'Start Node Attribute Value'])
    df_req2 = df_req2.reset_index()
    df_req2 = pd.merge(df_req1, df_req2)
    df_req2 = df_req2.sort_values(by='Record Tag')

    df_req2 = df_req2.reindex(
        columns=['Start Node Label', 'Start Node Attribute Name', 'Start Node Attribute Value', 'Graph Element', 
                 'Attribute Name', 'Attribute Value', 'Relation Name', 'Relation Attribute Name',
                 'Relation Attribute Value', 'End Node Label', 'End Node Attribute Name', 'End Node Attribute Value',
                 'Record Tag'])

final_dataset = df2.append(df_req2)
final_dataset = final_dataset.sort_values(by='Record Tag')
final_dataset['Attribute Value'] = final_dataset['Attribute Value'].apply(', '.join)
final_dataset['Attribute Value'] = final_dataset['Attribute Value'].apply(lambda cell: set([c.strip() for c in cell.split(',')]))
final_dataset['Attribute Value'] = final_dataset['Attribute Value'].apply(', '.join)
final_dataset['Attribute Value'] = final_dataset['Attribute Value'].str.lower()
final_dataset['Attribute Value'] = final_dataset['Attribute Value'].str.replace('\n', '')
final_dataset['End Node Attribute Value'] = (final_dataset['End Node Attribute Value'].str.split()).str.join(' ')


# Write data to a csv in data folder
final_dataset.to_csv(TRANSFORMED_DATA_FILE_PATH, index=False, encoding = 'utf-8')
print('Total time taken: %0.1f mins.\n' % ((time.time() - t0) / 60.0))

# Disease Synonym Generation
# data = data.loc[data['Start Node Label'] == 'Disease']
# data = data.loc[data['Graph Element'] == 'Attribute']
# df_temp = data.groupby(
#         ['Start Node Label', 'Start Node Attribute Name', 'Graph Element', 'Attribute Name',
#          'Start Node Attribute Value'])['Attribute Value'].unique()
#     df_tem_2 = data.groupby(['Record Tag', 'Start Node Label', 'Start Node Attribute Name', 'Graph Element', 'Attribute Name',
#          'Start Node Attribute Value'])['Attribute Value'].unique()
#     df_req1 = pd.DataFrame(df_temp)
#     df_req2 = pd.DataFrame(df_tem_2)
#     df_req1 = df_req1.reset_index()
#     df_req2 = df_req2.filter(['Record Tag', 'Start Node Label', 'Start Node Attribute Name', 'Graph Element', 'Attribute Name',
#          'Start Node Attribute Value'])
#     df_req2 = df_req2.reset_index()
#     df_req2 = pd.merge(df_req1, df_req2)
#     df_req2 = df_req2.sort_values(by='Record Tag')
#
#     df_req2 = df_req2.reindex(
#         columns=['Start Node Label', 'Start Node Attribute Name', 'Start Node Attribute Value', 'Graph Element',
#                  'Attribute Name', 'Attribute Value', 'Relation Name', 'Relation Attribute Name',
#                  'Relation Attribute Value', 'End Node Label', 'End Node Attribute Name', 'End Node Attribute Value',
#                  'Record Tag'])
#
# df_req2['Attribute Value'] = df_req2['Attribute Value'].apply(', '.join)
# df_req2['Attribute Value'] = df_req2['Attribute Value'].apply(lambda cell: set([c.strip() for c in cell.split(',')]))
# df_req2['Attribute Value'] = df_req2['Attribute Value'].apply(', '.join)
# df_req2['Attribute Value'] = df_req2['Attribute Value'].str.lower()