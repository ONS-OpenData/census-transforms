import os, shutil
import pandas as pd
from openpyxl import load_workbook
from databaker.framework import *


# assuming that all commission tables starting SP1 will be combined
# all commission tables starting SP2 will NOT be combined
#
# combine any tables first and then add metadata


class combine_and_add_metadata():
    
    def __init__(self):
        ##########
        self.commission_tables_tidy_data_location = "census-outputs/ct"
        self.outputs_tables_tidy_data_location = "census-outputs/outputs"
        self.cantabular_files_path = ""
        self.output_location = "census-outputs/final"
        self.commission_tables_metadata = "sp-data/commissioned tables small pops spec 09062023.xlsx"
        self.location_of_ct_source_files = "sp-data/ct"
        ##########
        
        self.commission_tables_files = [f for f in os.listdir(self.commission_tables_tidy_data_location) if not f.startswith('.')]
        self.outputs_tables_files = [f for f in os.listdir(self.outputs_tables_tidy_data_location) if not f.startswith('.')]
        
        self._create_dataset_dict()
        self.metadata_dict = {}
        
        self.commission_tables_count = len(self.commission_tables_files)
        self.outputs_tables_count = len(self.outputs_tables_files)
                
        
    def run(self):
        self._combine_outputs_tables()
        self._combine_commission_and_outputs_tables()
        self._get_metadata()
        self._add_metadata()
        self._print_outcomes()
        return
    
    def _create_dataset_dict(self):
        self.dataset_dict = {
            "commission_tables": {},
            "outputs_tables": {},
            "final": {}
            }
        
        for file in self.commission_tables_files:
            dataset = file.split('.')[0]
            to_combine = False
            if dataset.startswith('SP1'):
                to_combine = True
                combine_with = dataset[:-1]
            
            self.dataset_dict["commission_tables"][dataset] = {
                "to_combine": to_combine,
                "file": f"{self.commission_tables_tidy_data_location}/{file}"
                }
            if to_combine:
                self.dataset_dict["commission_tables"][dataset]["combine_with"] = combine_with
            
        for file in self.outputs_tables_files:
            dataset = file.split('.')[0]
            self.dataset_dict["outputs_tables"][dataset] = {
                "file": f"{self.outputs_tables_tidy_data_location}/{file}",
                "area": dataset.split('_')[0],
                "dataset_id": dataset.split('_')[1],
                "has_been_combined": False
                }
        return
            
    def _combine_outputs_tables(self):
        print("\nCombining outputs tables")
        count = 1
        
        for file in self.dataset_dict['outputs_tables']:
            print(f"\n**{file}** - {count} of {self.outputs_tables_count}")
            
            if self.dataset_dict['outputs_tables'][file]['has_been_combined'] == True:
                print(f"{file} has already been combined")
                count += 1
                continue
            
            dataset_id = self.dataset_dict['outputs_tables'][file]['dataset_id']
            
            # find any other files with same dataset_id
            combine_with_list = [file]
            self.dataset_dict['outputs_tables'][file]['has_been_combined'] = True
            
            for other_file in self.dataset_dict['outputs_tables']:
                if other_file == file:
                    continue
                
                other_dataset_id = self.dataset_dict['outputs_tables'][other_file]['dataset_id']
                if other_dataset_id == dataset_id:
                    combine_with_list.append(other_file)
                    self.dataset_dict['outputs_tables'][other_file]['has_been_combined'] = True
                  
            # copy files that dont need combining
            if len(combine_with_list) == 1:
                file_path = self.dataset_dict['outputs_tables'][file]['file']
                new_file_path = f"{self.output_location}/{self.dataset_dict['outputs_tables'][file]['dataset_id']}.xlsx"
                if os.path.exists(new_file_path):
                    os.remove(new_file_path)
                    
                shutil.copyfile(file_path, new_file_path)
                self.dataset_dict['final'][dataset_id] = {
                    'file': new_file_path,
                    'combined': []
                    }
                print(f"Outputs table {file} is not combining with another outputs table")
                count += 1
                continue
            
            # combining files
            print(f"sending outputs tables to combine - {combine_with_list}")
            self._combine_list_of_outputs_tables(combine_with_list)
            count += 1
          
        self.length_of_combined_outputs_tables = len(self.dataset_dict['final'])
        return
    
    def _combine_list_of_outputs_tables(self, list_of_tables):
        # combines a list of tables and writes it
        
        dataset_id = list_of_tables[0].split('_')[-1]
        output_file = f"{self.output_location}/{dataset_id}.xlsx"
        
        # quick check
        for i in range(1, len(list_of_tables)):
            assert list_of_tables[i].split('_')[-1] == dataset_id, f"trying to combine outputs tables with different dataset_ids - {dataset_id} & {list_of_tables[i].split('_')[-1]}"
          
        # orders tables from nat -> msoa -> ltla
        df_list = []
        combined_list_in_order = []
        if f"nat_{dataset_id}" in list_of_tables:
            dataset_file = self.dataset_dict['outputs_tables'][f"nat_{dataset_id}"]['file']
            xlsx = pd.ExcelFile(dataset_file)
            df_loop = pd.read_excel(xlsx, dtype=str)
            df_list.append(df_loop)
            combined_list_in_order.append(f"nat_{dataset_id}")
        
        if f"ltla_{dataset_id}" in list_of_tables:
            dataset_file = self.dataset_dict['outputs_tables'][f"ltla_{dataset_id}"]['file']
            xlsx = pd.ExcelFile(dataset_file)
            df_loop = pd.read_excel(xlsx, dtype=str)
            df_list.append(df_loop)
            combined_list_in_order.append(f"ltla_{dataset_id}")
            
        if f"msoa_{dataset_id}" in list_of_tables:
            dataset_file = self.dataset_dict['outputs_tables'][f"msoa_{dataset_id}"]['file']
            xlsx = pd.ExcelFile(dataset_file)
            df_loop = pd.read_excel(xlsx, dtype=str)
            df_list.append(df_loop)
            combined_list_in_order.append(f"msoa_{dataset_id}")
            
        df = pd.concat(df_list)
        df = self._df_column_tidy(df)
        
        # delete if exists
        self._delete(output_file)
        
        # write file
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Data', index=False, header=False)
            
        print(f"outputs tables combined {combined_list_in_order}")
        self.dataset_dict['final'][dataset_id] = {
            'file': output_file,
            'combined': combined_list_in_order
            }
        return
    
    def _combine_commission_and_outputs_tables(self):
        # combine commission tables with outputs tables
        print("\nCombining commission tables with outputs tables")
        count = 1
        
        for dataset in self.dataset_dict['commission_tables']:
            print(f"\n**{dataset}** - {count} of {self.commission_tables_count}")
            
            if self.dataset_dict['commission_tables'][dataset]['to_combine']:
                dataset_file = self.dataset_dict['commission_tables'][dataset]['file']
                xlsx = pd.ExcelFile(dataset_file)
                df = pd.read_excel(xlsx, dtype=str)
                
                dataset_to_combine = self.dataset_dict['commission_tables'][dataset]['combine_with']
                dataset_file_to_combine = f"{self.output_location}/{dataset_to_combine}.xlsx"
                if not os.path.exists(dataset_file_to_combine):
                    raise FileNotFoundError(f"Trying to combine {dataset} with {dataset_to_combine} but file does not exist - {dataset_file_to_combine}")
                
                xlsx_to_combine = pd.ExcelFile(dataset_file_to_combine)
                df_to_combine = pd.read_excel(xlsx_to_combine, dtype=str)
                
                assert len(df.columns) == len(df_to_combine.columns), "Column lengths for df & df_to_combine do not match"
                for i in range(len(df.columns)):
                    assert df.columns[i] == df_to_combine.columns[i], f"df col {df.columns[i]} does not match df_to_combine col {df_to_combine.columns[i]}"
                    
                # combining the df's
                new_df = pd.concat([df, df_to_combine])
                new_df = self._df_column_tidy(new_df)
                
                print(f"Combining commission table {dataset} with outputs table {dataset_to_combine}")
                
                # delete if exists
                self._delete(dataset_file_to_combine)
                
                # write to dataset_file_to_combine
                with pd.ExcelWriter(dataset_file_to_combine, engine='openpyxl') as writer:
                    new_df.to_excel(writer, sheet_name='Data', index=False, header=False)
                    
                self.dataset_dict['final'][dataset_to_combine]['combined'].append(dataset)
                
            else:
                # write files that are not being combined
                file_path = self.dataset_dict['commission_tables'][dataset]['file']
                new_file_path = f"{self.output_location}/{dataset}.xlsx"
                shutil.copyfile(file_path, new_file_path)
                
                print(f"Commission table {dataset} not combining with any output tables")
                
                self.dataset_dict['final'][dataset] = {
                    'file': new_file_path,
                    'combined': []
                    }
            
            count += 1
            
        self.length_of_combined_ct_outputs_tables = len(self.dataset_dict['final'])
        return
    
    def _df_column_tidy(self, dataframe):
        # putting column names in df to avoid bold headers in excel
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index+1  
        dataframe.loc[0] = dataframe.columns
        dataframe = dataframe.sort_index()
        return dataframe
    
    def _delete(self, file):
        # delete if exists
        if os.path.exists(file):
            os.remove(file)
        return
    
    def _get_metadata(self):
        # gets all the metadata 
        print("Fetching metadata")
        
        dataset_df = pd.read_csv(f"{self.cantabular_files_path}/Dataset.csv")
        commission_metadata_df1 = pd.read_excel(f"{self.commission_tables_metadata}", sheet_name="EILR")
        commission_metadata_df2 = pd.read_excel(f"{self.commission_tables_metadata}", sheet_name="COB")
        
        for dataset_id in self.dataset_dict['final']:
            if dataset_id.startswith("SP2") and dataset_id.endswith('H') or dataset_id.startswith("SP2") and dataset_id.endswith('G') or dataset_id in ("SP115A", "SP116A", "SP117A", "SP118A", "SP119A"):
                df_loop = commission_metadata_df1[commission_metadata_df1[' table number'] == dataset_id]
                
                self.metadata_dict[dataset_id] = {}
                self.metadata_dict[dataset_id]['dataset_title'] = df_loop['table title'].iloc[0]
                self.metadata_dict[dataset_id]['dataset_description'] = df_loop['dataset_description / Table Notes'].iloc[0]
                self.metadata_dict[dataset_id]['dataset_statistical_unit'] = "Person"
                self.metadata_dict[dataset_id]['dataset_population'] = self._get_dataset_population(dataset_id) # TODO - check works against spreadsheets
                
            
            elif dataset_id.startswith("SP1") or dataset_id.startswith("SP2"):
                df_loop = dataset_df[dataset_df['Dataset_Mnemonic'] == dataset_id]
                
                if len(df_loop) == 0:
                    self.dataset_dict['final'][dataset_id]['real_id'] = dataset_id[:-1]
                else:
                    self.dataset_dict['final'][dataset_id]['real_id'] = dataset_id
                    
                df_loop = dataset_df[dataset_df['Dataset_Mnemonic'] == self.dataset_dict['final'][dataset_id]['real_id']]
                
                self.metadata_dict[dataset_id] = {}
                self.metadata_dict[dataset_id]['dataset_title'] = df_loop['Dataset_Title'].iloc[0]
                self.metadata_dict[dataset_id]['dataset_description'] = df_loop['Dataset_Description'].iloc[0]
                self.metadata_dict[dataset_id]['dataset_statistical_unit'] = df_loop['Statistical_Unit'].iloc[0] 
                self.metadata_dict[dataset_id]['dataset_population'] = df_loop['Dataset_Population'].iloc[0]                        
                    
            else:
                raise TypeError(f"{dataset_id} should start SP1 or SP2")
        
        del dataset_df
        
        source_df = pd.read_csv(f"{self.cantabular_files_path}/Source.csv")
        statement = source_df['SDC_Statement'].iloc[0]
        
        for dataset_id in self.metadata_dict:
            self.metadata_dict[dataset_id]['source_sdc_statement'] = statement    
        
        del source_df, statement
        
        dataset_variable_df = pd.read_csv(f"{self.cantabular_files_path}/Dataset_Variable.csv")
        
        for dataset_id in self.metadata_dict:
            if dataset_id.startswith("SP2") and dataset_id.endswith('H') or dataset_id.startswith("SP2") and dataset_id.endswith('G') or dataset_id in ("SP115A", "SP116A", "SP117A", "SP118A", "SP119A"):
                
                df_loop = commission_metadata_df1[commission_metadata_df1[' table number'] == dataset_id]
                
                self.metadata_dict[dataset_id]['area_types'] = {}
                self.metadata_dict[dataset_id]['variables'] = {}
                
                variables_list = df_loop['variables'].iloc[0]
                if variables_list.startswith('Flat classification'):
                    variables_list = variables_list.split('Flat classification for ethnic group, ')[-1]
                
                variables_list = variables_list.split(',')
                variables_list = [variable.strip().lower() for variable in variables_list]
                
                for variable in variables_list:
                    is_correct_variable_name = True
                    for item in variable:
                        if item.isnumeric():
                            is_correct_variable_name = False
                            
                    if is_correct_variable_name:
                        correct_variable_name = variable
                    else:
                        correct_variable_name = '_'.join(variable.split('_')[:-1])
                    
                    self.metadata_dict[dataset_id]['variables'][correct_variable_name] = {}
                    self.metadata_dict[dataset_id]['variables'][correct_variable_name]['classification'] = variable
                    
                areas_list = df_loop['Geography'].iloc[0]
                areas_list = areas_list.split('/')
                lookup = {'national': 'nat', 'country': 'ctry', 'region': 'rgn'}
                areas_list = [lookup.get(area.lower(), area.lower()) for area in areas_list]
                
                for area in areas_list:
                    self.metadata_dict[dataset_id]['area_types'][area] = {}
            
            elif dataset_id.startswith("SP1") or dataset_id.startswith("SP2"):
            
                df_loop = dataset_variable_df[dataset_variable_df['Dataset_Mnemonic'] == self.dataset_dict['final'][dataset_id]['real_id']]
                
                self.metadata_dict[dataset_id]['area_types'] = {}
                self.metadata_dict[dataset_id]['variables'] = {}
                
                for code in df_loop['Variable_Mnemonic'].unique():
                    df_loop_2 = df_loop[df_loop['Variable_Mnemonic'] == code]
                    
                    if pd.isnull(df_loop_2['Lowest_Geog_Variable_Flag'].iloc[0]):
                        self.metadata_dict[dataset_id]['variables'][code] = {}
                        self.metadata_dict[dataset_id]['variables'][code]['classification'] = df_loop_2['Classification_Mnemonic'].iloc[0]
                        
                    else:
                        self.metadata_dict[dataset_id]['area_types'][code] = {}
                        
                if self.dataset_dict['final'][dataset_id]['combined'] != []:
                    combined_datasets = self.dataset_dict['final'][dataset_id]['combined']
                    for extra_dataset_id in combined_datasets:
                        df_loop = commission_metadata_df1[commission_metadata_df1[' table number'] == extra_dataset_id]
                        if len(df_loop) == 0:
                            df_loop = commission_metadata_df2[commission_metadata_df2[' table number'] == extra_dataset_id]
                            
                        if len(df_loop) == 0:
                            # means it is looking for outputs table in ct spreadsheet
                            continue
                        areas_list = df_loop['Geography'].iloc[0]
                        areas_list = areas_list.split('/')
                        lookup = {'national': 'nat', 'country': 'ctry', 'region': 'rgn'}
                        areas_list = [lookup.get(area.lower(), area.lower()) for area in areas_list]
                        
                        for area in areas_list:
                            self.metadata_dict[dataset_id]['area_types'][area] = {}
                    
        
        del dataset_variable_df, commission_metadata_df1, commission_metadata_df2
        
        variable_df = pd.read_csv(f"{self.cantabular_files_path}/Variable.csv")
        
        for dataset_id in self.metadata_dict:
            for area in self.metadata_dict[dataset_id]['area_types']:
                df_loop = variable_df[variable_df['Variable_Mnemonic'] == area]
                
                self.metadata_dict[dataset_id]['area_types'][area]['title'] = df_loop['Variable_Title'].iloc[0]
                self.metadata_dict[dataset_id]['area_types'][area]['description'] = df_loop['Variable_Description'].iloc[0]
                
            for variable in self.metadata_dict[dataset_id]['variables']:
                df_loop = variable_df[variable_df['Variable_Mnemonic'] == variable]
                
                self.metadata_dict[dataset_id]['variables'][variable]['title'] = df_loop['Variable_Title'].iloc[0]
                self.metadata_dict[dataset_id]['variables'][variable]['description'] = df_loop['Variable_Description'].iloc[0]
                self.metadata_dict[dataset_id]['variables'][variable]['quality_statement'] = df_loop['Quality_Statement_Text'].iloc[0]
                if df_loop['Quality_Summary_URL'].iloc[0] == '':
                    continue
                elif pd.isnull(df_loop['Quality_Summary_URL'].iloc[0]):
                    continue
                else:
                    topic = df_loop['Topic_Mnemonic'].iloc[0]
                    if topic == 'DEM':
                        text = 'Read more in our Demography and migration quality information for Census 2021 methodology'
                    elif topic == 'MIG':
                        text = 'Read more in our Demography and migration quality information for Census 2021 methodology'
                    elif topic == 'LAB':
                        text = 'Read more in our Labour market quality information for Census 2021 methodology'
                    elif topic == 'HOU':
                        text = 'Read more in our housing quality information for Census 2021 methodology'
                    elif topic == 'HUC':
                        text = 'Read more in our Health, disability and unpaid care quality information for Census 2021 methodology'
                    elif topic == 'EILR':
                        text = 'Read more in our Ethnic group, national identity, language and religion quality information for Census 2021 methodology'
                    elif topic == 'EDU':
                        text = 'Read more in our Education quality information for Census 2021 methodology'
                    else:
                        raise NotImplementedError(f"{topic} - not included yet")
                    
                    self.metadata_dict[dataset_id]['variables'][variable]['quality_statement_url'] = f"=HYPERLINK(\"{df_loop['Quality_Summary_URL'].iloc[0]}\", \"{text}\")"
                
        del variable_df
        
        category_df = pd.read_csv(f"{self.cantabular_files_path}/Category.csv")
        classification_df = pd.read_csv(f"{self.cantabular_files_path}/Classification.csv")
        
        for dataset_id in self.metadata_dict:
            for variable in self.metadata_dict[dataset_id]['variables']:
                classification = self.metadata_dict[dataset_id]['variables'][variable]['classification']
                df_loop = category_df[category_df['Classification_Mnemonic'] == classification]
                category_dict = dict(zip(df_loop['External_Category_Label_English'], df_loop['Category_Code']))
                self.metadata_dict[dataset_id]['variables'][variable]['category'] = category_dict
                df_loop = classification_df[classification_df['Classification_Mnemonic'] == classification]
                self.metadata_dict[dataset_id]['variables'][variable]['classification_label'] = df_loop['External_Classification_Label_English'].iloc[0]
                
        del category_df, classification_df            
            
        return 
    
    def _parse_metadata(self, dataset_id):
        # parses metadata into required format
        
        area_details = []
        # lowest geog first
        if 'msoa' in self.metadata_dict[dataset_id]['area_types'].keys():
            area_details.append(self.metadata_dict[dataset_id]['area_types']['msoa']['title'])
        if 'ltla' in self.metadata_dict[dataset_id]['area_types'].keys():
            area_details.append(self.metadata_dict[dataset_id]['area_types']['ltla']['title'])
        if 'rgn' in self.metadata_dict[dataset_id]['area_types'].keys():
            area_details.append(self.metadata_dict[dataset_id]['area_types']['rgn']['title'])
        if 'ctry' in self.metadata_dict[dataset_id]['area_types'].keys():
            area_details.append(self.metadata_dict[dataset_id]['area_types']['ctry']['title'])
        if 'nat' in self.metadata_dict[dataset_id]['area_types'].keys():
            area_details.append(self.metadata_dict[dataset_id]['area_types']['nat']['title'])
                                
        variables_list = []
        for variable in self.metadata_dict[dataset_id]['variables']:
            if pd.isnull(self.metadata_dict[dataset_id]['variables'][variable]['quality_statement']):
                has_quality_info = False
            elif self.metadata_dict[dataset_id]['variables'][variable]['quality_statement'] == "":
                has_quality_info = False
            else:
                has_quality_info = True
                
            if pd.isnull(self.metadata_dict[dataset_id]['variables'][variable]['quality_statement_url']):
                has_quality_url = False
            elif self.metadata_dict[dataset_id]['variables'][variable]['quality_statement_url'] == "":
                has_quality_url = False
            else:
                has_quality_url = True
                
            details = ['Variable Name', self.metadata_dict[dataset_id]['variables'][variable]['title']]
            description = ['Variable Description', self.metadata_dict[dataset_id]['variables'][variable]['description']]
            variables_list.append(details)
            variables_list.append(description)
            
            if has_quality_info:
                variables_list.append(['Quality Note(s)', self.metadata_dict[dataset_id]['variables'][variable]['quality_statement']])
            
            if has_quality_url:
                variables_list.append(['Quality Statement URL', self.metadata_dict[dataset_id]['variables'][variable]['quality_statement_url']])
            
        
        rows_of_data = [
                    ['Metadata Field', 'Metadata Content'],
                    ['Title', self.metadata_dict[dataset_id]['dataset_title']],
                    ['Description', self.metadata_dict[dataset_id]['dataset_description']],
                    ['Release Date', '25/09/2023'],
                    ['Dataset Population', self.metadata_dict[dataset_id]['dataset_population']],
                    ['Unit of Measure', self.metadata_dict[dataset_id]['dataset_statistical_unit']],
                    ['Contact Email', 'census.customerservices@ons.gov.uk'],
                    ['Contact Telephone Number', '+44 1329 444972'],
                    ['Statistical Disclosure Control Statement', self.metadata_dict[dataset_id]['source_sdc_statement']],
                    ['Area Types', ', '.join(area_details)], 
                    ['Area Type Summary', """Census 2021 statistics are published for a number of different geographies. These can be large, for example the whole of England, or small, for example an output area (OA), the lowest level of geography for which statistics are produced.
For higher levels of geography, more detailed statistics can be produced. When a lower level of geography is used, such as output areas (which have a minimum of 100 persons), the statistics produced have less detail. This is to protect the confidentiality of people and ensure that individuals or
their characteristics cannot be identified."""],
                ]
        
        for item in variables_list:
            rows_of_data.append(item)
        
        rows_of_data.extend(
                [
                    ['Version Number', '1'],
                    ['Related Content Title', 'Small Populations'],
                    ['Related Content Description', 'Small population tables provide census data for some of the key characteristics of people in specific small population groups - for example individuals of an ethnic group, a country of birth, a religion or a national identity - in which the small size of the total population in that group means confidentiality constraints limit the release of more detailed standard statistics.'],
                    ['Related Content URL', '=HYPERLINK("https://www.nomisweb.co.uk/sources/census_2021_sp")'],
                    ['Related Content Title', 'Small population groups, England and Wales: Census 2021'],
                    ['Related Content Description', 'Statistics about small population groups, Census 2021 data'],
                    ['Related Content URL', '=HYPERLINK("https://www.ons.gov.uk/releases/smallpopulationsenglandandwalescensus2021")'],
                    ['Related Content Title', 'Census 2021 dictionary'],
                    ['Related Content Description', 'Definitions, variables and classifications to help when using Census 2021 data.'],
                    ['Related Content URL', '=HYPERLINK("https://www.ons.gov.uk/census/census2021dictionary")'],
                    ['Source', 'Office for National Statistics © Crown Copyright 2023'],
                    ['Copyright Statement and Terms and Conditions', ''],
                    ['Terms and Conditions', 'All material on the Office for National Statistics (ONS) website is subject to Crown Copyright protection unless otherwise indicated. These statistics may be used, excluding logos, under the terms of the Open Government Licence.'],
                    ['Licence URL', '=HYPERLINK("http://www.nationalarchives.gov.uk/doc/open-government-licence/")'],
                    ['', ''],
                    
                ]
            )
        
        return rows_of_data
    
    def _add_metadata(self):
        # parses and then adds the metadata for a given dataset to the tidy excel file
        for dataset_id in self.metadata_dict:
            try:
                # getting parsed metdata
                rows_of_data = self._parse_metadata(dataset_id)
                # creating dataframe of metadata
                df = pd.DataFrame(rows_of_data, columns=['A', 'B'])
                
                # adding metadata to file
                dataset_file = self.dataset_dict['final'][dataset_id]['file']
                book = load_workbook(dataset_file)
                writer = pd.ExcelWriter(dataset_file, engine='openpyxl')
                writer.book = book
                df.to_excel(writer, sheet_name='Metadata', header=False, index=False)
                writer.close()
                
                print(f"Attached metadata for {dataset_id}")
                
            except Exception as e:
                print(f"Error in _add_metadata for {dataset_id}")
                print(e)
        
        return
    
    def _get_dataset_population(self, dataset_id):
        commission_metadata_df = pd.read_excel(f"{self.commission_tables_metadata}", sheet_name="EILR")
        
        if dataset_id.endswith('H'): # Caribbean data
            id_to_use = 'SP219H'
        elif dataset_id.endswith('G'):
            id_to_use = 'SP219G'
        elif dataset_id in ("SP115A", "SP116A", "SP117A", "SP118A", "SP119A"):
            id_to_use = dataset_id
        else:
            raise Exception(f"_get_dataset_population - dataset {dataset_id} is trying to find dataset population from spreadsheet rather than model")
        
        df = commission_metadata_df[commission_metadata_df[' table number'] == id_to_use]
        assert len(df) == 1
        value = df['table population'].iloc[0]
        value = value.split(':')[0].strip()
        
        return value
    
    def _print_outcomes(self): 
        print(f"{self.outputs_tables_count} outputs tables combined into {self.length_of_combined_outputs_tables}")
        print(f"{self.commission_tables_count} commission tables & {self.length_of_combined_outputs_tables} combined outputs tables combined into {self.length_of_combined_ct_outputs_tables} final table")
        return


if __name__ == '__main__':
    combine = combine_and_add_metadata()
    combine.run()
    
    