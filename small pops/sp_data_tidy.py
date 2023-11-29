import os
import pandas as pd
from openpyxl import load_workbook

class run_outputs():
    
    def __init__(self):
        ##########
        self.location_of_source_files = "sp-data/outputs" 
        self.output_location = "census-test-outputs/outputs" 
        self.cantabular_files_path = "" # to be filled
        ##########
        
        if self.output_location == "/":
            self.output_location = ""
            
        self.source_files = [f for f in os.listdir(self.location_of_source_files) if not f.startswith('.')]
        self._get_area_metadata()
        self._create_dict()
        
        self.tidy_data_incomplete = []
        self.number_of_files = len(self.source_files) 
        
    def run(self):
        self._tidy_data()
        self._print_outcomes()
        return
    
    def _tidy_data(self):
        for dataset in self.dataset_dict:
            count = 1
            print(f"Tidying data for {dataset} - {count} of {self.number_of_files}")
            try:
                df = pd.read_csv(self.dataset_dict[dataset]['source_file'], dtype=str)
                if 'Percentage' in df.columns:
                    df = df.drop(['Percentage'], axis=1)
                
                area_lookup = self.metadata_dict['area_type']
                def area_type_label(value):
                    return area_lookup[value]
                
                df['Geography Code'] = df[df.columns[0]]
                df['Geography Label'] = df[df.columns[1]]
                df['Area type'] = area_type_label(self.dataset_dict[dataset]['area_type'])
                
                new_codelist_order = ['Geography Code', 'Geography Label', 'Area type']
                for i in range(2, len(df.columns) - 3):
                    new_codelist_order.append(df.columns[i])
                    
                df = df[new_codelist_order]
                
                assert df.columns[-1] == "Count", f"{dataset} - last columns should be 'Count' not {df.columns[-1]}"
                
                # putting column names in df to avoid bold headers in excel
                df = df.reset_index(drop=True)
                df.index = df.index+1  
                df.loc[0] = df.columns
                df = df.sort_index()
                
                with pd.ExcelWriter(self.dataset_dict[dataset]['output_file'], engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Data', index=False, header=False)
                    
                print(f"{dataset} - tidy data")
                
            except Exception as e:
                print(f"Error in _tidy_data for {dataset}")
                print(e)
                self.tidy_data_incomplete.append(f"{dataset}")
    
    def _create_dict(self):
        self.dataset_dict = {}
        for file in self.source_files:
            dataset = file.split('.csv')[0]
            sp_code = dataset.split('_')[-1]
            self.dataset_dict[dataset] = {
                    'source_file': f'{self.location_of_source_files}/{file}',
                    'sp_code': sp_code,
                    'area_type': dataset.split('_')[0],
                    'output_file': f'{self.output_location}/{dataset}.xlsx'
                        }
            
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
    
    def _print_outcomes(self):            
        if self.tidy_data_incomplete != []:
            print("_tidy_data that errored")
            print(self.tidy_data_incomplete, '\n')
            
        else:
            print("All outputs tables have been transformed into tidy data")
            

if __name__ == '__main__':
    sp_tidy_data = run_outputs()
    sp_tidy_data.run()
    
