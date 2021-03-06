FEC_PREFIX = 'FEC_'

# Indicates that this committee was filed as part of the bulk data for a given cycle
BASIC_FILING_STATUS = '%sACTIVE'

# Map FEC committee designations to OCD type tags
FEC_DESIGNATION_PREFIX = 'DESIG_'
COMMITTEE_DESIGNATIONS = {
  'A': '%s%sCANDIDATE_AUTHORIZED' % (FEC_PREFIX, FEC_DESIGNATION_PREFIX),
  'B': '%s%sPAC_LOBBYIST' % (FEC_PREFIX, FEC_DESIGNATION_PREFIX),
  'D': '%s%sPAC_LEADERSHIP' % (FEC_PREFIX, FEC_DESIGNATION_PREFIX),
  'J': '%s%sJOINT_FUNDRAISER' % (FEC_PREFIX, FEC_DESIGNATION_PREFIX),
  'P': '%s%sCANDIDATE_PRINCIPAL' % (FEC_PREFIX, FEC_DESIGNATION_PREFIX),
  'U': '%s%sUNAUTHORIZED' % (FEC_PREFIX, FEC_DESIGNATION_PREFIX),
}

# Map FEC committee types to OCD type tags
FEC_TYPE_PREFIX = 'TYPE_'
COMMITTEE_TYPES = {
  'C': '%s%sCOMMUNICATION_COST' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'D': '%s%sDELEGATE_COMMITTEE' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'E': '%s%sELECTIONEERING_COMMUNICATION' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'H': '%s%sCAMPAIGN_HOUSE' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'I': '%s%sINDEPENDENT_EXPENDITOR' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'N': '%s%sPAC_NONQUALIFIED' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'O': '%s%sPAC_SUPER' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'P': '%s%sCAMPAIGN_PRESIDENT' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'Q': '%s%sPAC_QUALIFIED' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'U': '%s%sINDEPENDENT_SINGLE' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'S': '%s%sCAMPAIGN_SENATE' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'V': '%s%sPAC_OTHER' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'W': '%s%sPAC_OTHER' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'X': '%s%sPARTY_NONQUALIFIED' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'Y': '%s%sPARTY_QUALIFIED' % (FEC_PREFIX, FEC_TYPE_PREFIX),
  'Z': '%s%sPARTY_NONFEDERAL' % (FEC_PREFIX, FEC_TYPE_PREFIX)
}

# Map FEC committee party IDs to full names
FEC_COMMITTEE_PREFIX = 'CMTE_'
COMMITTEE_PARTIES = {
  'ACE': 'Ace Party ',
  'AKI': 'Alaskan Independence Party',
  'AIC': 'American Independent Conservative',
  'AIP': 'American Independent Party',
  'AMP': 'American Party',
  'APF': 'American People\'s Freedom Party',
  'AE': 'Americans Elect',
  'CIT': 'Citizens\' Party',
  'CMD': 'Commandments Party',
  'CMP': 'Commonwealth Party of the U.S.',
  'COM': 'Communist Party',
  'CNC': 'Concerned Citizens Party Of Connecticut',
  'CRV': 'Conservative Party',
  'CON': 'Constitution Party',
  'CST': 'Constitutional Party',
  'COU': 'Country Party',
  'DCG': 'D.C. Statehood Green Party',
  'DNL': 'Democratic-Nonpartisan League',
  'DEM': 'Democratic Party',
  'D/C': 'Democratic/Conservative Party',
  'DFL': 'Democratic-Farmer-Labor Party',
  'DGR': 'Desert Green Party',
  'FED': 'Federalist Party',
  'FLP': 'Freedom Labor Party',
  'FRE': 'Freedom Party',
  'GWP': 'George Wallace Party',
  'GRT': 'Grassroots Party',
  'GRE': 'Green Party',
  'GR': 'Green-Rainbow Party',
  'HRP': 'Human Rights Party',
  'IDP': 'Independence Party',
  'IND': 'Independent Party',
  'IAP': 'Independent American Party',
  'ICD': 'Independent Conservative Democratic Party',
  'IGR': 'Independent Green Party',
  'IP': 'Independent Party',
  'IDE': 'Independent Party of Delaware',
  'IGD': 'Industrial Government Party',
  'JCN': 'Jewish/Christian National Party',
  'JUS': 'Justice Party',
  'LRU': 'La Raza Unida',
  'RUP': 'La Raza Unida',
  'LBR': 'Labor Party',
  'LAB': 'Labor Party',
  'LFT': 'Less Federal Taxes Party',
  'LBL': 'Liberal Party',
  'LIB': 'Libertarian Party',
  'LBU': 'Liberty Union Party',
  'MTP': 'Mountain Party',
  'NDP': 'National Democratic Party',
  'NLP': 'Natural Law Party',
  'NA': 'New Alliance Party',
  'NJC': 'New Jersey Conservative Party',
  'NPP': 'New Progressive Party',
  'NPA': 'No Party Affiliation',
  'NOP': 'No Party Preference',
  'NNE': 'None',
  'N': 'Nonpartisan',
  'NON': 'Non-Party',
  'OE': 'One Earth Party',
  'OTH': 'Other',
  'PG': 'Pacific Green',
  'PSL': 'Party for Socialism and Liberation',
  'PAF': 'Peace And Freedom Party',
  'PFP': 'Peace And Freedom Party',
  'PFD': 'Peace Freedom Party',
  'POP': 'People Over Politics Party',
  'PPY': 'People\'s Party',
  'PCH': 'Personal Choice Party',
  'PPD': 'Popular Democratic Party',
  'PRO': 'Progressive Party',
  'NAP': 'Prohibition Party',
  'PRI': 'Puerto Rican Independence Party',
  'REF': 'Reform Party',
  'REP': 'Republican Party',
  'RES': 'Resource Party',
  'RTL': 'Right To Life Party',
  'SEP': 'Socialist Equality Party',
  'SLP': 'Socialist Labor Party',
  'SUS': 'Socialist Party',
  'SOC': 'Socialist Party U.S.A.',
  'SWP': 'Socialist Workers Party',
  'TX': 'Taxpayers Party',
  'TWR': 'Taxpayers Without Representation Party',
  'TEA': 'Tea Party',
  'THD': 'Theo-Democratic Party',
  'USP': 'U.S. People\'s Party',
  'UST': 'U.S. Taxpayers Party',
  'UN': 'Unaffiliated',
  'UC': 'United Citizen',
  'UNI': 'United Party',
  'UNK': 'Unknown',
  'VET': 'Veterans Party',
  'WTP': 'We the People Party',
  'W': 'Write-In Party'
}

# Committee officers
TREASURER = '%sTREASURER' % FEC_PREFIX
