select count(1) from occurrences where sample_id = %s and gnomad_af < 0.05 and gnomad_af > 0.0;