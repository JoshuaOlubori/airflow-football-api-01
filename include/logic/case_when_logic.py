def apply_case_when_logic(df):
    columns_to_process = ['teams_did_not_score',"both_teams_scored","won_3_games_or_more", "draw",
        'over_0_5', 'over_1_5', 'over_2_5', 'over_3_5',
        'under_0_5', 'under_1_5', 'under_2_5', 'under_3_5', 'under_4_5'
    ]
    
    for col in columns_to_process:
        df[col] = df[col].apply(lambda x: "T" if x == 1 else ("F" if x == 0 else None))
    
    return df