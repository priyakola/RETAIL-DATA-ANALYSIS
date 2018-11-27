mysql -uroot -p mainproject -e "LOAD DATA INFILE '/home/cloudera/Desktop/header.csv' INTO TABLE header FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n' IGNORE 1 LINES (InvH_icode, InvH_BillNo, InvH_Billdate, InvH_pty_icode, InvH_counter, InvH_username, InvH_optdisSameDiff, InvH_disper, InvH_pymtmode, InvH_pymtmodedet, InvH_basamt, InvH_lessdisamt, InvH_billnett, InvH_scstchr, InvH_roff, InvH_collamt, InvH_cashrecd, InvH_cashret, InvH_bank_icode, InvH_cou_type_icode, InvH_cheNo, InvH_cheDate, InvH_cheAmt, InvH_cashpaidAmt, InvH_crecardno, InvH_crecardAmt, InvH_denom_total, InvH_CancelFlag, InvH_prilcust_icode, fin_yr, usr_icode, comp_icode, ser_date)";



mysql -uroot -p datasets -e "LOAD DATA LOCAL INFILE '/home/cloudera/Desktop/detail.csv' INTO TABLE detail FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n'";




sqoop import --connect jdbc:mysql://localhost:3306/datasets -username root -P --table header -target-dir /user/cloudera/header_final_ds -m1


sqoop import --connect jdbc:mysql://localhost:3306/datasets -username root -P --table detail -target-dir /user/cloudera/detail_final_ds -m1