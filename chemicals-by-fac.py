from ECHO_modules.get_data import get_echo_data
from ECHO_modules.rsei_utilities import add_chemical_to_submissions, get_this_by_that
import pandas as pd

chemicals = "'1,2,4-Trichlorobenzene', '1,2,4-Trimethylbenzene', 'Benzene', 'Hexachlorobenzene', "
chemicals += "'Mercury', 'Mercury compounds', 'Polychlorinated biphenyls ', 'Polycyclic aromatic compounds', "
chemicals += "'Ethylene oxide'"
# chemical_numbers = "51, 321, 359, 360, 474, 564, 575, 609, 293"
chemical_numbers = "'51', '321', '359', '360', '474', '564', '575', '609', '293'"
start_year = 2017
end_year = 2024

# Get the submissions 
columns = '"SubmissionNumber", "FacilityID", "ChemicalNumber", "SubmissionYear", "OneTimeReleaseQty", "TradeSecretInd"'

# sql = f'select {columns} from "submissions_data_rsel_v2312" where "SubmissionYear" >= \'{start_year}\' and "SubmissionYear" <= \'{end_year}\''
# sql += f' and "ChemicalNumber" in ({chemical_numbers})'
# sql += ' and "ChemicalNumber" = 51'
# sql = 'select "SubmissionYear" from "submissions_data_rsel_v2312" where "ChemicalNumber" = 51'
sql = 'select "SubmissionNumber", "FacilityID", "ChemicalNumber", "SubmissionYear", "OneTimeReleaseQty", "TradeSecretInd" '
sql += 'from "submissions_data_rsei_v2312" where "ChemicalNumber" in (51,321,359,360,474,564,575,609,293)'
sql += ' and "SubmissionYear" >= \'2017\''

sub_df = get_echo_data(sql)
sub_df = sub_df[sub_df['SubmissionYear'] >= 2017]

sub_df.to_csv('All-submissions.csv')

# sub_df = pd.read_csv('All-submissions.csv')
columns = '"ChemicalNumber", "Chemical", "RfCInhale"'
sub_df = add_chemical_to_submissions(submissions=sub_df, chemical_columns=columns)

# Get the releases for the submissions
columns = '"ReleaseNumber", "SubmissionNumber", "Media", "PoundsReleased", "OffsiteNumber", "TEF"'
filter = None
# if chosen_media is not None:
#     filter = {'filter_field' : 'Media', 'filter_list' : chosen_media['Media'].to_list(), 'int_flag' : True}
rel_df = get_this_by_that(this_name='releases', that_series=sub_df['SubmissionNumber'], this_key='SubmissionNumber',
                          this_columns=columns, filter = filter)
print(f'{len(rel_df)} releases found for these submissions')
rel_df.to_csv('All-releases.csv')

# rel_df = pd.read_csv('All-releases.csv')

# Get the elements for the releases
columns = '"ReleaseNumber", "ElementNumber", "PoundsPT", "ScoreCategory", "Score", "Population", "ScoreA", "PopA", "ScoreB", "PopB"'
element_df = get_this_by_that(this_name='elements', that_series=rel_df['ReleaseNumber'], this_key='ReleaseNumber', 
                              this_columns=columns)
print(f'{len(element_df)} elements found for these releases')
element_df.to_csv('All-elements.csv')

# sub_df = pd.read_csv('Exemptions-submissions.csv')
# rel_df = pd.read_csv('Exemptions-releases.csv')
# element_df = pd.read_csv('All-submissions-elements-4-chemicals.csv')

# Add the elements to the releases dataframe
link_df1 = sub_df.set_index('SubmissionNumber').join(rel_df.set_index('SubmissionNumber'))
all_df = link_df1.set_index('ReleaseNumber').join(element_df.set_index('ReleaseNumber'), 
                                                how='left', lsuffix='_l', rsuffix='_r')
fac_chemicals = all_df[all_df.index.notnull()]
fac_chemicals.to_csv('All-submissions-elements-4-chemicals.csv')