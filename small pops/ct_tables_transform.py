import os, datetime, math, json
import pandas as pd
from databaker.framework import *

class run_transforms_commission_tables():
    
    def __init__(self, transforms_to_run="*"): 
        # runs based on whether .py script exists for the dataset
        
        ##########        
        self.location_of_scripts = "census-transforms" 
        self.location_of_source_files = "sp-data/ct"
        self.output_location = "census-outputs/ct" 
        self.cantabular_files_path = ""
        self.commission_tables_metadata = "sp-data/commissioned tables small pops spec 09062023.xlsx"
        ##########
        
        if self.location_of_scripts.endswith("/"):
            self.location_of_scripts = self.location_of_scripts[:-1]
            
        if self.output_location == "/":
            self.output_location = ""
            
        self.transforms_to_run = transforms_to_run # can be a list or an individual code
        
        if type(self.transforms_to_run) == str:
            self.transforms_to_run = [self.transforms_to_run]
        
        self.transform_files = [f for f in os.listdir(self.location_of_scripts) if not f.startswith('.')]
        self._get_transform_files()
        
        self.transform_status = {}
        self._get_area_metadata()
        
        self.run_scripts_incomplete = []
        self.tidy_data_incomplete = []
        self.add_metadata_incomplete = []
        
        self.number_of_scripts = len(self.transform_files) 
        
    def run(self):
        self._run_scripts() 
        self._get_metadata()
        self._tidy_data()
        self._print_outcomes()
        
        return
    
    def _tidy_data(self):
        # will open excel data file, sort columns and dimensions (add code columns), turn into required tidy data format
        for dataset_id in self.transform_status:
            count = 1
            print(f"Tidying data for {dataset_id} - {count} of {self.number_of_scripts}")
            try:
                dataset_file = self.transform_status[dataset_id]['output_file']
                xlsx = pd.ExcelFile(dataset_file)
                df = pd.read_excel(xlsx, dtype=str)
            
                area_lookup = self.metadata_dict['area_type']
                def area_type_label(value):
                    return area_lookup[value]
                
                # create code columns
                # re-order columns
                # rename columns
                # get dimension codes and apply them
                
                new_column_order = []
                
                for col in df.columns:
                    if col == 'OBS':
                        df['Count'] = df['OBS']
                    
                    elif col == 'small_population':
                        df['Geography Code'] = df['small_population'].apply(lambda x: x.split(' ')[0])
                        new_column_order.append('Geography Code')
                        
                        df['Geography Label'] = df['small_population'].apply(lambda x: ' '.join(x.split(' ')[1:]))
                        new_column_order.append('Geography Label')
                    
                    elif col == 'area_type':
                        df['Area type'] = df['area_type'].apply(area_type_label)
                        new_column_order.append('Area type')
                    
                    else:
                        variable = col.split(' ')[0]
                        variable_label = self.metadata_dict[dataset_id]['variables'][variable]['classification_label']
                        
                        label_to_code_dict = self.metadata_dict[dataset_id]['variables'][variable]['category'].copy()
                        
                        def label_to_codes(value):
                            if value not in label_to_code_dict.keys():
                                raise Exception(f"{value} not found in label_to_code_dict")
                            
                            return label_to_code_dict[value]
                        
                        df[f'{variable_label} Code'] = df[col].apply(label_to_codes)
                        new_column_order.append(f'{variable_label} Code')
                        
                        df[f'{variable_label} Label'] = df[col] 
                        new_column_order.append(f'{variable_label} Label')
                        
                        del label_to_code_dict
                        
                new_column_order.append('Count')
                df = df[new_column_order]
                
                # putting column names in df to avoid bold headers in excel
                df = df.reset_index(drop=True)
                df.index = df.index+1  
                df.loc[0] = df.columns
                df = df.sort_index()
                # making column headers not bold
                #pd.io.formats.excel.ExcelFormatter.header_style = None
                
                with pd.ExcelWriter(dataset_file, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Data', index=False, header=False)
                    
                print(f"{dataset_id} now in tidy data format")
                    
            except Exception as e:
                print(f"Error in _tidy_data for {dataset_id}")
                print(e)
                self.tidy_data_incomplete.append(f"{dataset_id}")
                
            count += 1
        
        return 
    
    def _run_scripts(self):
        # runs selected transform(s)
        count = 1
        for transform in self.transform_files:
            print(f"Running transform on {transform} - {count} of {self.number_of_scripts}")
            loc = {}
            with open(f"{self.location_of_scripts}/{transform}.py") as f:
                script = f.read()
                
            script = script.replace("source_location = \"\"", f"source_location = \"{self.location_of_source_files}\"")
            script = script.replace("output_location = \"\"", f"output_location = \"{self.output_location}\"")
            try:
                exec(script, globals(), loc)
                self.transform_status.update(loc['output'])
            except Exception as e:
                print(f"Error in _run_scripts for {transform}")
                print(e)
                self.run_scripts_incomplete.append(transform)
                
            count += 1
        
        return
    
    def _get_transform_files(self):
        # gets the list of transform files to run
        new_transform_files = []
        
        if self.transforms_to_run == ["*"]:
            for transform in self.transform_files:
                new_transform_files.append(transform.split('.py')[0])
            
        else:
            for transform in self.transforms_to_run:
                if f"{transform}.py" in self.transform_files:
                    new_transform_files.append(transform)
                
                else:
                    print(f"No transform exists for {transform}") 
        
        self.transform_files = new_transform_files
        
        return
    
    def _get_area_metadata(self):
        # gets some inital metadata for area types
        self.metadata_dict = {}
        self.metadata_dict['area_type'] = {}
        variable_df = pd.read_csv(f"{self.cantabular_files_path}/Variable.csv")
        df = variable_df[variable_df['Variable_Type_Code'] == 'GEOG']
        for code in df['Variable_Mnemonic'].unique():
            df_loop = df[df['Variable_Mnemonic'] == code]
            self.metadata_dict['area_type'][code] = df_loop['Variable_Title'].iloc[0]
        return
    
    def _get_metadata(self):
        # gets all the metadata 
        print("Fetching metadata")
        
        dataset_df = pd.read_csv(f"{self.cantabular_files_path}/Dataset.csv")
        commission_metadata_df = pd.read_excel(f"{self.commission_tables_metadata}", sheet_name="EILR")
        
        for dataset_id in self.transform_status:
            if dataset_id.startswith("SP2") or dataset_id in ("SP115A", "SP116A", "SP117A", "SP118A", "SP119A"):
                df_loop = commission_metadata_df[commission_metadata_df[' table number'] == dataset_id]
                
                self.metadata_dict[dataset_id] = {}
                self.metadata_dict[dataset_id]['dataset_title'] = df_loop['table title'].iloc[0]
                self.metadata_dict[dataset_id]['dataset_description'] = df_loop['dataset_description / Table Notes'].iloc[0]
                self.metadata_dict[dataset_id]['dataset_statistical_unit'] = "Person"
            
            elif dataset_id.startswith("SP1"):
                df_loop = dataset_df[dataset_df['Dataset_Mnemonic'] == dataset_id]
                
                if len(df_loop) == 0:
                    self.transform_status[dataset_id]['real_id'] = dataset_id[:-1]
                else:
                    self.transform_status[dataset_id]['real_id'] = dataset_id
                    
                df_loop = dataset_df[dataset_df['Dataset_Mnemonic'] == self.transform_status[dataset_id]['real_id']]
                
                self.metadata_dict[dataset_id] = {}
                self.metadata_dict[dataset_id]['dataset_title'] = df_loop['Dataset_Title'].iloc[0]
                self.metadata_dict[dataset_id]['dataset_description'] = df_loop['Dataset_Description'].iloc[0]
                self.metadata_dict[dataset_id]['dataset_statistical_unit'] = df_loop['Statistical_Unit'].iloc[0]   
                
            else:
                raise TypeError(f"{dataset_id} should start SP1 or SP2")
        
        del dataset_df
        
        source_df = pd.read_csv(f"{self.cantabular_files_path}/Source.csv")
        statement = source_df['SDC_Statement'].iloc[0]
        
        for dataset_id in self.metadata_dict:
            if dataset_id == 'area_type':
                continue
            
            self.metadata_dict[dataset_id]['source_sdc_statement'] = statement    
        
        del source_df, statement
        
        dataset_variable_df = pd.read_csv(f"{self.cantabular_files_path}/Dataset_Variable.csv")
        
        for dataset_id in self.metadata_dict:
            if dataset_id == 'area_type':
                continue
            
            if dataset_id.startswith("SP2") or dataset_id in ("SP115A", "SP116A", "SP117A", "SP118A", "SP119A"):
                
                df_loop = commission_metadata_df[commission_metadata_df[' table number'] == dataset_id]
                
                self.metadata_dict[dataset_id]['area_types'] = {}
                self.metadata_dict[dataset_id]['variables'] = {}
                
                variables_list = df_loop['variables'].iloc[0]
                if variables_list.startswith('Flat classification'):
                    variables_list = variables_list.split('Flat classification for ethnic group, ')[-1]
                
                variables_list = variables_list.split(',')
                variables_list = [variable.strip().lower() for variable in variables_list]
                
                for variable in variables_list:
                    if variable == '':
                        continue
                    is_correct_variable_name = True
                    for item in variable:
                        if item.isnumeric():
                            is_correct_variable_name = False
                            
                    if is_correct_variable_name:
                        correct_variable_name = variable
                    else:
                        correct_variable_name = '_'.join(variable.split('_')[:-1])
                        if correct_variable_name == 'economic_activity_status':
                            correct_variable_name = 'economic_activity'
                    
                    self.metadata_dict[dataset_id]['variables'][correct_variable_name] = {}
                    self.metadata_dict[dataset_id]['variables'][correct_variable_name]['classification'] = variable
                    
                areas_list = df_loop['Geography'].iloc[0]
                areas_list = areas_list.split('/')
                lookup = {'national': 'nat', 'country': 'ctry', 'region': 'rgn'}
                areas_list = [lookup.get(area.lower(), area.lower()) for area in areas_list]
                
                for area in areas_list:
                    self.metadata_dict[dataset_id]['area_types'][area] = {}
            
            elif dataset_id.startswith("SP1"):
            
                df_loop = dataset_variable_df[dataset_variable_df['Dataset_Mnemonic'] == self.transform_status[dataset_id]['real_id']]
                
                self.metadata_dict[dataset_id]['area_types'] = {}
                self.metadata_dict[dataset_id]['variables'] = {}
                
                for code in df_loop['Variable_Mnemonic'].unique():
                    df_loop_2 = df_loop[df_loop['Variable_Mnemonic'] == code]
                    
                    if pd.isnull(df_loop_2['Lowest_Geog_Variable_Flag'].iloc[0]):
                        self.metadata_dict[dataset_id]['variables'][code] = {}
                        self.metadata_dict[dataset_id]['variables'][code]['classification'] = df_loop_2['Classification_Mnemonic'].iloc[0]
                        
                    else:
                        self.metadata_dict[dataset_id]['area_types'][code] = {}
                        
        del dataset_variable_df, commission_metadata_df
        
        variable_df = pd.read_csv(f"{self.cantabular_files_path}/Variable.csv")
        
        for dataset_id in self.metadata_dict:
            if dataset_id == 'area_type':
                continue
            
            print(variable)
            
            for area in self.metadata_dict[dataset_id]['area_types']:
                df_loop = variable_df[variable_df['Variable_Mnemonic'] == area]
                
                self.metadata_dict[dataset_id]['area_types'][area]['title'] = df_loop['Variable_Title'].iloc[0]
                self.metadata_dict[dataset_id]['area_types'][area]['description'] = df_loop['Variable_Description'].iloc[0]
                
            for variable in self.metadata_dict[dataset_id]['variables']:
                print(variable)
                if dataset_id == 'SP117A' and variable == 'religion_detailed':
                    continue

                df_loop = variable_df[variable_df['Variable_Mnemonic'] == variable]
                
                self.metadata_dict[dataset_id]['variables'][variable]['title'] = df_loop['Variable_Title'].iloc[0]
                self.metadata_dict[dataset_id]['variables'][variable]['description'] = df_loop['Variable_Description'].iloc[0]
                self.metadata_dict[dataset_id]['variables'][variable]['quality_statement'] = df_loop['Quality_Statement_Text'].iloc[0]
                self.metadata_dict[dataset_id]['variables'][variable]['quality_statement_url'] = df_loop['Quality_Summary_URL'].iloc[0]
                
        del variable_df
        
        category_df = pd.read_csv(f"{self.cantabular_files_path}/Category.csv")
        classification_df = pd.read_csv(f"{self.cantabular_files_path}/Classification.csv")
        
        for dataset_id in self.metadata_dict:
            if dataset_id == 'area_type':
                continue
            
            for variable in self.metadata_dict[dataset_id]['variables']:                
                classification = self.metadata_dict[dataset_id]['variables'][variable]['classification']
                df_loop = category_df[category_df['Classification_Mnemonic'] == classification]
                category_dict = dict(zip(df_loop['External_Category_Label_English'], df_loop['Category_Code']))
                self.metadata_dict[dataset_id]['variables'][variable]['category'] = category_dict
                df_loop = classification_df[classification_df['Classification_Mnemonic'] == classification]
                self.metadata_dict[dataset_id]['variables'][variable]['classification_label'] = df_loop['External_Classification_Label_English'].iloc[0]
                
        del category_df, classification_df            
            
        return     
    
    def create_new_transform(self, new_dataset_ids):
        base_script = """
dataset_code = ''

# both variables need to be here for source files to be found & to give output location
# do not touch
source_location = "" 
output_location = ""

output_lookup = {
        'National': 'nat',
        'Region': 'rgn',
        'Country': 'ctry',
        'MSOA': 'msoa',
        'LTLA': 'ltla',
        }

#file = f"{source_location}/{dataset_code}_.xlsx" 
output_file = f"{output_location}/{dataset_code}.xlsx"

tabs = loadxlstabs(file)
tabs = [tab for tab in tabs if 'METADATA' not in tab.name]

df_list, area_codes, obs_count_check = [], [], []
for tab in tabs:
    #obs_column = 'D'
    #start_point_row_number = '10'
    junk = tab.excel_ref('A').filter(contains_string('Created on')).expand(DOWN)
    
    list_of_geogs = tab.excel_ref(f"A{start_point_row_number}").fill(DOWN).is_not_blank().is_not_whitespace() - junk    
    number_of_geogs = len(list_of_geogs)
    #number_to_jump = len_of_dim1 * len_of_dim2
    
    len_of_obs = len(tab.excel_ref(f"{obs_column}{start_point_row_number}").fill(DOWN).is_not_blank().is_not_whitespace())
    obs_count_check.append(len_of_obs)
    
    area_code = output_lookup[' '.join(tab.name.split(' ')[1:])]
    start_point = tab.excel_ref(f'A{start_point_row_number}')
    
    for i, geog in enumerate(list_of_geogs):
        if i+1 == number_of_geogs:
            Min = str(geog.y + 1)
            Max = str(tab.excel_ref('A').filter(contains_string('Created on')).y)
            
        else:
            Min = str(geog.y + 1)
            
            rest_of_geogs = tab.excel_ref(f"A{str(int(Min)+1)}:A{str(int(Min) + number_to_jump)}").is_not_blank().is_not_whitespace() - junk
            for j, next_geog in enumerate(rest_of_geogs):
                Max = str(next_geog.y)
                break
    
        geography = tab.excel_ref(f"A{Min}:A{Max}").is_not_blank().is_not_whitespace()
        geography -= junk
        
        #dim1 = tab.excel_ref(f"B{Min}:B{Max}").is_not_blank().is_not_whitespace()
        
        #dim2 = tab.excel_ref(f"C{Min}:C{Max}").is_not_blank().is_not_whitespace()
        
        obs = tab.excel_ref(f"{obs_column}{Min}:{obs_column}{Max}").is_not_blank().is_not_whitespace()
        
        if len(obs) != 0:
            dimensions = [
                    HDim(geography, 'small_population', CLOSEST, ABOVE),
                    HDimConst('area_type', area_code),
                    #HDim(dimension1, 'dimension1 label',  CLOSEST, ABOVE),
                    #HDim(dimension2, 'dimension2 label', DIRECTLY, LEFT),
                    ]
            
            cs = ConversionSegment(tab, dimensions, obs).topandas()
            df_list.append(cs) 
    
    area_codes.append(area_code)

df = pd.concat(df_list)
assert len(df) == sum(obs_count_check), f"df length - {len(df)} does not match sum of obs {sum(obs_count_check)}"

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df.to_excel(writer, sheet_name='data', index=False)
    
print(f"{dataset_code} - transform complete")

output = {dataset_code: {"output_file": output_file, "area_types": area_codes}}

"""
        
        if type(new_dataset_ids) == str:
            new_dataset_ids = [new_dataset_ids]
            
        assert type(new_dataset_ids) == list, "new_dataset_ids must be a list"
        
        # get dimensions used in data
        dataset_variable_df = pd.read_csv(f"{self.cantabular_files_path}/Dataset_Variable.csv")
        commission_metadata_df = pd.read_excel(f"{self.commission_tables_metadata}", sheet_name="EILR")
        
        for dataset_id in new_dataset_ids:
            script = base_script
            if os.path.isfile(f"{self.location_of_scripts}/{dataset_id}.py"):
                print(f"{self.location_of_scripts}/{dataset_id}.py already exists")
                continue
            
            if dataset_id.startswith('SP1'):
            
                df_loop = dataset_variable_df[dataset_variable_df['Dataset_Mnemonic'] == dataset_id]
                if len(df_loop) == 0:
                    df_loop = dataset_variable_df[dataset_variable_df['Dataset_Mnemonic'] == dataset_id[:-1]]
                
                table_dimensions = []
                for code in df_loop['Variable_Mnemonic'].unique():
                    df_loop_2 = df_loop[df_loop['Variable_Mnemonic'] == code]
                    if pd.isnull(df_loop_2['Lowest_Geog_Variable_Flag'].iloc[0]):
                        table_dimensions.append(code)
                        
            elif dataset_id.startswith('SP2') or dataset_id in ("SP115A", "SP116A", "SP117A", "SP118A", "SP119A"):
                
                df_loop = commission_metadata_df[commission_metadata_df[' table number'] == dataset_id]
                
                variables_list = df_loop['variables'].iloc[0]
                if variables_list.startswith('Flat classification'):
                    variables_list = variables_list.split('Flat classification for ')[-1]
                
                variables_list = variables_list.split(',')
                variables_list = [variable.strip().lower() for variable in variables_list]
                table_dimensions = variables_list
                    
            script = script.replace("dataset_code = ''", f"dataset_code = '{dataset_id}'")
            script += "# for use in building transform\n"
            script += f"table_dimensions = {table_dimensions}"
            
            with open(f"{self.location_of_scripts}/{dataset_id}.py", "w") as f:
                f.write(script)
                f.close()
            print(f"New transform written for - {dataset_id}")
         
        return
    
    def _print_outcomes(self):
        if self.number_of_scripts == 0:
            return
        
        if self.run_scripts_incomplete != []:
            print("_run_scripts that errored")
            print(self.run_scripts_incomplete, '\n')
            
        else:
            print("All commission tables have been transformed")
            
        if self.tidy_data_incomplete != []:
            print("_tidy_data that errored")
            print(self.tidy_data_incomplete, '\n')
            
        else:
            print("All commission tables have been transformed into tidy data")
            
        
        

if __name__ == '__main__':
    transform_object = run_transforms_commission_tables(
            transforms_to_run=['*']
            )
    transform_object.run()
    # to create a new transform unhash below:
    #new_transforms = ['SP101']
    #transform_object.create_new_transform(new_transforms)
    
    