select vep_impact, count(1) from occurrences where sample_id = %s group by vep_impact;