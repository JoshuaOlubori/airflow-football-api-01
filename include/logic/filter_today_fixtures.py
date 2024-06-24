def filter_fixtures_today(df):
            """
            Function to return only fixtures playing today. 
            For logging purposes. Not part of the core logic
            """
            # Get the current date in UTC format
            current_date_utc = pd.to_datetime(datetime.utcnow())


            df['date'] = pd.to_datetime(
                df['date']).dt.date
            df['date'] = pd.to_datetime(
                df['date']).dt.date


            df.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_all_days_across_71_leagues.csv"))
            gv.task_log.info("Saved all fixtures successfully \n")
            
            # Filter DataFrames based on the current date
            filtered_df_today = df[df['date'] == current_date_utc.date(
            )]
            filtered_df_today = filtered_df_today.drop_duplicates()
            
            filtered_df_today.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_today_across_71_leagues.csv"))
            gv.task_log.info("Save successful. Showing today's fixtures. \n")

            gv.task_log.info(filtered_df_today.drop(columns=["season", "country", "date"], errors='ignore'))
