CREATE OR REPLACE PROCEDURE dxo.orion_interface_load (t_jobid IN NUMBER)
IS
t_householdid		NUMBER;
t_contactid         NUMBER;
t_phoneid           NUMBER;
t_addressid         NUMBER;
t_webaddid          NUMBER;
t_accountid         NUMBER;
t_positionid        NUMBER;
t_secmasterid       NUMBER;
t_pricehistoryid    NUMBER;
t_sosecmasterid     NUMBER;
t_extension			NUMBER;
t_clientid			NUMBER;
t_appuserid         NUMBER;
t_count1            NUMBER;
t_count2            NUMBER;
t_count4            NUMBER;
t_price				NUMBER;
t_expirationdate	DATE;
t_day_part			VARCHAR2(5);
t_month_part		VARCHAR2(5);
t_year_part			VARCHAR2(5);
t_expdate_string	VARCHAR2(30);
t_taxid				VARCHAR2(30);
t_phonenum  		VARCHAR2(30);
t_phonenumber		VARCHAR2(30);
t_zero              varchar2(30);
t_prodvacode        varchar2(32);
t_webaddress		VARCHAR2(64);
t_errmsg			VARCHAR2(200);
t_errcode			VARCHAR2(200);
BEGIN
	SELECT appuserid INTO t_appuserid FROM dxo.das_job WHERE jobid = t_jobid;

	-- Processing for Household
	FOR C1 IN (SELECT * FROM dxo.orion_household_inv_stg WHERE jobid = t_jobid AND flag = 0)
	LOOP
		-- Processing for Contact
		BEGIN
			SELECT CONTACTID INTO t_contactid FROM DXO.ORION_CONTACT_INV_INT WHERE JOBID = T_JOBID AND CLIENTID = C1.CLIENTID;
		EXCEPTION WHEN NO_DATA_FOUND THEN
			BEGIN
				DXO.GETPKVALUE('ORION_CONTACT_INV_INT',t_contactid);
				INSERT INTO dxo.orion_contact_inv_int  (contactid, jobid, personalid, jobtitle, clientid, taxid, firstname, dob, contacttitle, lastname, gender, appuserid, employer, recordtype, subsystemtype, contacttype, ppstatusmsg)
				VALUES (t_contactid, t_jobid, c1.personalid, c1.jobtitle, c1.clientid, c1.taxid, c1.firstname, TO_DATE(C1.DOB, 'YYYY/MM/DD'), c1.contacttitle, c1.lastname, dxo.GetOrionGender(c1.gender), t_appuserid, c1.company, 1, 1, c1.clientcategory, 'Successful');
				COMMIT;

				-- Processing for Address
				DXO.GETPKVALUE('ORION_ADDRESS_INV_INT',t_addressid);
				INSERT INTO dxo.ORION_ADDRESS_INV_INT (ADDRESSID, JOBID, CONTACTID, COUNTRY, STATE, POSTALCODE, CITY, ADDRLINE3, ADDRLINE2, ADDRLINE1)
				VALUES(t_addressid, t_jobid, t_contactid, C1.COUNTRY, C1.STATE, C1.ZIP, C1.CITY, C1.ADDRLINE3, C1.ADDRLINE2, C1.ADDRLINE1);
				COMMIT;

				-- Processing for Phone
				FOR j IN 1..5
				LOOP
					select replace(regexp_replace(decode(j,1,c1.fax,2,c1.homephone,3,c1.mobilephone,4,c1.busphone,5,c1.othphone), '[0]', ''),' ',''), replace(decode(j,1,c1.fax,2,c1.homephone,3,c1.mobilephone,4,c1.busphone,5,c1.othphone),' ','')
					into t_zero, t_phonenum from dual;
					-- countrycode(areacode)dialnumber_preferred-extension
					IF t_zero IS NOT NULL and t_phonenum IS NOT NULL then
						IF LENGTH(regexp_replace(t_phonenum, '[^0-9]', '')) = 10 AND INSTR(t_phonenum,'x') = 0 THEN
							--If the data is of 10 digits, the first 3 digits will be considered as Area Code and remaining 7 as Dial Number
							IF INSTR(t_phonenum,'(') = 0 AND INSTR(t_phonenum,')') = 0 THEN
								SELECT '('||SUBSTR(t_phonenum,1,3) ||')'||SUBSTR(t_phonenum,4)||'_'||DECODE(j,1,1,0) INTO t_phonenumber FROM dual;
							ELSIF INSTR(t_phonenum,'(') > 0 AND INSTR(t_phonenum,')') > 0 THEN
								SELECT SUBSTR(t_phonenum,INSTR(t_phonenum,'('),INSTR(t_phonenum,')'))||SUBSTR(t_phonenum,INSTR(t_phonenum,')')+1)||'_'||DECODE(j,1,1,0) INTO t_phonenumber FROM dual;
							END IF;
						ELSIF LENGTH(regexp_replace(t_phonenum, '[^0-9]', '')) > 10 THEN
							IF INSTR(t_phonenum,'x') > 0 THEN
								--If the data is more than 10 digits with value containing 'x' in it, the value before "x" will be Area Code and Dial Number and value after "x" will be extension
								IF INSTR(t_phonenum,'(') = 0 AND INSTR(t_phonenum,')') = 0 THEN
									SELECT '('||SUBSTR(t_phonenum,1,3) ||')'||SUBSTR(t_phonenum,4,INSTR(t_phonenum,'x')-4)||'_'||DECODE(j,1,1,0)||'-'||SUBSTR(t_phonenum,INSTR(t_phonenum,'x')+1) INTO t_phonenumber FROM dual;
								ELSIF INSTR(t_phonenum,'(') > 0 AND INSTR(t_phonenum,')') > 0 THEN
									SELECT SUBSTR(t_phonenum,1,INSTR(t_phonenum,'(')-1) ||SUBSTR(t_phonenum,INSTR(t_phonenum,'('),INSTR(t_phonenum,')'))||SUBSTR(t_phonenum,INSTR(t_phonenum,')')+1,
									INSTR(SUBSTR(t_phonenum,INSTR(t_phonenum,')')+1,INSTR(t_phonenum,'x')-1),'x')-1)||'_'||DECODE(j,1,1,0)||'-'||SUBSTR(t_phonenum,INSTR(t_phonenum,'x')+1) INTO t_phonenumber FROM dual;
								END IF;
							ELSIF INSTR(t_phonenum,'x') = 0 THEN
								--If the data is more than 10 digits (no reference for extension), last 10 digits of it will be considered as Area Code and Dial Number and remaining will be considered as Country Code.
								IF INSTR(t_phonenum,'(') = 0 AND INSTR(t_phonenum,')') = 0 THEN
									SELECT SUBSTR(t_phonenum,1,INSTR(t_phonenum,SUBSTR(t_phonenum,-10))-1)||'('||SUBSTR(t_phonenum,-10,3)||')'||SUBSTR(t_phonenum,-7)||'_'||DECODE(j,1,1,0) INTO t_phonenumber FROM dual;
								ELSIF INSTR(t_phonenum,'(') > 0 AND INSTR(t_phonenum,')') > 0 THEN
									SELECT SUBSTR(t_phonenum,1,INSTR(t_phonenum,SUBSTR(t_phonenum,-10))-1)||SUBSTR(t_phonenum,-10,3)||SUBSTR(t_phonenum,-7)||'_'||DECODE(j,1,1,0) INTO t_phonenumber FROM dual;
								END IF;
							END IF;
						ELSE
							t_phonenumber := NULL;
						END IF;

						DXO.GETPKVALUE('ORION_PHONE_INV_INT',t_phoneid);
						INSERT INTO dxo.orion_phone_inv_int (phoneid, jobid, contactid, phonenumber, phonetype, extension )
						VALUES(t_phoneid, t_jobid, t_contactid, t_phonenumber, DECODE(j,1,4,2,1,3,10,4,2,5,14), replace(decode(j,2,c1.resphext,4,c1.busphextn,5,c1.othphextn,NULL),' ',''));
						COMMIT;

						IF j = 1 THEN
							UPDATE DXO.ORION_CONTACT_INV_INT SET BusinessFax = t_phonenumber where jobid = t_jobid and contactid = t_contactid;
						ELSIF j = 2  THEN
							UPDATE DXO.ORION_CONTACT_INV_INT SET homephone = t_phonenumber where jobid = t_jobid and contactid = t_contactid;
						ELSIF j = 3 THEN
							UPDATE DXO.ORION_CONTACT_INV_INT SET mobilephone = t_phonenumber where jobid = t_jobid and contactid = t_contactid;
						ELSIF j = 4  THEN
							UPDATE DXO.ORION_CONTACT_INV_INT SET businessphone = t_phonenumber where jobid = t_jobid and contactid = t_contactid;
						ELSIF j = 5 THEN
							UPDATE DXO.ORION_CONTACT_INV_INT SET otherphone = t_phonenumber where jobid = t_jobid and contactid = t_contactid;
						END IF;
						COMMIT;
					END IF;
				END LOOP;-- j

				-- Processing for Webaddress
				FOR j IN 1..2
				LOOP
					SELECT DECODE(j,1,c1.email,2,c1.webaddress, NULL) INTO t_webaddress FROM DUAL;
					IF t_webaddress IS NOT NULL THEN
						DXO.GETPKVALUE('ORION_WEBADDRESS_INV_INT',t_webaddid);
						INSERT INTO dxo.ORION_WEBADDRESS_INV_INT (WEBADDID, JOBID, CONTACTID, ADDRESS, TYPE)
						VALUES(t_webaddid, t_jobid, t_contactid, t_webaddress, DECODE(j,1,1,2,2));
						COMMIT;
					END IF;
				END LOOP; --j
			EXCEPTION WHEN OTHERS THEN
				t_errmsg := SUBSTR(SQLERRM||', '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,200);
				t_errcode := SQLCODE;
				UPDATE DXO.ORION_CONTACT_INV_INT SET PPSTATUS = 2, PPSTATUSMSG = t_errcode||' : '||t_errmsg WHERE JOBID = T_JOBID AND CONTACTID = t_contactid;
				COMMIT;
			END;
		END; -- Contact

		-- Household
		BEGIN
			dxo.getpkvalue('ORION_HOUSEHOLD_INV_INT',t_householdid);
			INSERT INTO dxo.orion_household_inv_int(householdid, jobid, contactid, clientid, name, appuserid, personalid, memberrole, sourceofdatatype, networth, ppstatusmsg)
			VALUES (t_householdid, t_jobid, t_contactid, c1.clientid,  c1.name, t_appuserid, c1.personalid, 1, 3, c1.aum, 'Successful');
			COMMIT;
		EXCEPTION WHEN OTHERS THEN
			t_errmsg := SUBSTR(SQLERRM||', '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,200);
			t_errcode := SQLCODE;
			UPDATE DXO.ORION_HOUSEHOLD_INV_INT SET PPSTATUS = 2, PPSTATUSMSG = t_errcode||' : '||t_errmsg WHERE JOBID = T_JOBID AND HOUSEHOLDID = t_householdid;
			COMMIT;
		END;
		
		-- Update flag 1 in stg table (Processed)
		UPDATE dxo.orion_household_inv_stg SET flag = 1 WHERE jobid = t_jobid AND householdid = c1.householdid;
		COMMIT;
	END LOOP; -- C1

	-- Processing for Accounts
	FOR C2 IN (SELECT * FROM dxo.orion_account_inv_stg WHERE jobid = t_jobid  AND UPPER(ACCOUNTSTATUS) = UPPER('True') AND acctnumber IS NOT NULL AND flag = 0)
	LOOP
		BEGIN
			BEGIN
				SELECT contactid, taxid INTO t_contactid, t_taxid FROM dxo.orion_contact_inv_int WHERE jobid = t_jobid AND clientid = c2.acctclientid;
			EXCEPTION
				WHEN NO_DATA_FOUND THEN
					t_contactid := NULL;
					t_taxid := NULL;
			END;
			
			--Note: Set PPSTATUS value 2 for "123456890" this account number
			dxo.getpkvalue('ORION_ACCOUNT_INV_INT',t_accountid);
			INSERT INTO dxo.orion_account_inv_int  (accountid, jobid, contactid, accountnumber, orionacctid, acctstatusdesc, acctopendate, accountstatus, accounttype, accountname, acctcloseddate, custodian, qualifiedacct, appuserid, taxid, clientid,dataprovider, ppstatus, ppstatusmsg)
			VALUES (t_accountid, t_jobid, t_contactid, c2.acctnumber, c2.orionacctid, c2.acctstatusdesc, TO_DATE(c2.acctstartdate,'YYYY/MM/DD'), DECODE(c2.accountstatus,'true',1,'false',0,NULL), c2.accounttype, c2.accountname, TO_DATE(c2.acctcloseddate,'YYYY/MM/DD'), c2.custodian, DECODE(c2.qualifiedacct,'true',1,'false',0,NULL), t_appuserid, t_taxid, c2.acctclientid, 'Orion', (CASE c2.acctnumber WHEN '123456890' THEN 2 ELSE 1 END),(CASE c2.acctnumber WHEN '123456890' THEN 'Test account number excluded as per request' ELSE 'Successful' END));
			COMMIT;
		EXCEPTION
			WHEN OTHERS THEN
			t_errmsg := SUBSTR(SQLERRM||', '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,200);
			t_errcode := SQLCODE;
			UPDATE dxo.orion_account_inv_int SET ppstatus = 2, ppstatusmsg = t_errcode||' : '||t_errmsg WHERE jobid = t_jobid AND accountid = t_accountid;
			COMMIT;
		END;
		
		-- Update flag 1 in stg table (Processed)
		UPDATE dxo.orion_account_inv_stg SET FLAG = 1 WHERE jobid = t_jobid and acctid = c2.acctid;
		COMMIT;
	END LOOP; --C2

	-- Processing for Positions
	FOR C3 IN (SELECT * FROM dxo.orion_position_inv_stg WHERE JOBID = t_jobid AND UPPER(POSITIONSTATUS) = UPPER('True')AND SHARES NOT IN (0,0.0) AND currentvalue NOT IN (0,0.0) AND accountnumber IS NOT NULL AND flag = 0)
	LOOP
		BEGIN
			SELECT CONTACTID INTO T_CONTACTID FROM DXO.ORION_CONTACT_INV_INT WHERE JOBID = T_JOBID AND CLIENTID = C3.CLIENTID;
		EXCEPTION
			WHEN NO_DATA_FOUND THEN
			t_contactid := NULL;
		END;

		-- Skeleton (Account)
		SELECT count(*) INTO t_count4 FROM DXO.ORION_ACCOUNT_INV_INT WHERE JOBID = T_JOBID AND ORIONACCTID = C3.ACCOUNTID AND ACCOUNTNUMBER = C3.ACCOUNTNUMBER;
		IF t_count4 = 0 THEN
			DXO.GETPKVALUE('ORION_ACCOUNT_INV_INT',t_accountid);
			INSERT INTO dxo.ORION_ACCOUNT_INV_INT  (ACCOUNTID,JOBID,ORIONACCTID,ACCOUNTNUMBER, CONTACTID, ACCOUNTSTATUS, DATAPROVIDER) VALUES (t_accountid, T_JOBID, C3.ACCOUNTID, C3.ACCOUNTNUMBER, T_CONTACTID, 1, 'Orion');
			COMMIT;
		END IF;
		-- Positions
		IF DXO.GetOrionHoldingtype(C3.PRODUCTTYPE, c3.PRODSUBTYPE) = 1003 THEN
			T_PRICE := (C3.PRICE * 100);
		ELSE
			T_PRICE := C3.PRICE;
		END IF;

		BEGIN
			--Note: Set PPSTATUS value 2 for "123456890" this account number.
			DXO.GETPKVALUE('ORION_POSITION_INV_INT',t_positionid);
			INSERT INTO dxo.ORION_POSITION_INV_INT (POSITIONID, JOBID, CONTACTID, ORIONACCTID, INVESTMENTNAME, SHARES, HOLDINGTYPE, SYMBOL, CLIENTID, PRICE, ACCOUNTNUMBER, CURRENTVALUE, APPUSERID, CUSIP, PPSTATUS, PPSTATUSMSG)
			VALUES (t_positionid, t_jobid, T_CONTACTID, C3.ACCOUNTID, C3.NAME, C3.SHARES, DXO.GetOrionHoldingtype(C3.PRODUCTTYPE, c3.PRODSUBTYPE), C3.SYMBOL, C3.CLIENTID, REPLACE(T_PRICE,'null',''), C3.ACCOUNTNUMBER, C3.CURRENTVALUE, t_appuserid, C3.CUSIP, (CASE C3.accountnumber WHEN '123456890' THEN 2 ELSE 1 END),(CASE c3.accountnumber WHEN '123456890' THEN 'Test account number excluded as per request' ELSE 'Successful' END));
			COMMIT;
		EXCEPTION
			WHEN OTHERS THEN
			t_errmsg := SUBSTR(SQLERRM||', '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,200);
			t_errcode := SQLCODE;
			UPDATE DXO.ORION_POSITION_INV_INT SET PPSTATUS = 2, PPSTATUSMSG = t_errcode||' : '||t_errmsg WHERE JOBID = T_JOBID AND POSITIONID = t_positionid;
			COMMIT;
		END;
	
		-- Update flag 1 in stg table (Processed)
		UPDATE dxo.orion_position_inv_stg SET FLAG = 1 WHERE jobid = t_jobid and positionid =  c3.positionid;
		COMMIT;
	END LOOP; --C3

	FOR C4 IN (SELECT * FROM dxo.ORION_SECMASTER_INV_STG WHERE JOBID = t_jobid AND FLAG = 0)
	LOOP
		BEGIN
			-- Processing for Secmaster
			select count(*) into t_count1 FROM dxo.ORION_SECMASTER_INV_INT WHERE JOBID = t_jobid AND ((CUSIP = C4.CUSIP AND SYMBOL = C4.TICKER) OR (CUSIP = C4.CUSIP));
			IF t_count1 = 0 THEN

				IF DXO.GetOrionProducttype(C4.PRODUCTTYPE, C4.PRODSUBTYPE) = 1003 THEN
					T_PRICE := (C4.PRICE * 100);
				ELSE
					T_PRICE := C4.PRICE;
				END IF;

				--Expiration date conversion to "DD-MM-YYYY"
				t_day_part := SUBSTR(c4.expirationdate, 1, INSTR(c4.expirationdate, '-') - 1); 
				t_month_part := SUBSTR(c4.expirationdate, INSTR(c4.expirationdate, '-') + 1, INSTR(c4.expirationdate, '-', INSTR(c4.expirationdate, '-') + 1) - INSTR(c4.expirationdate, '-') - 1);
				t_year_part := SUBSTR(c4.expirationdate, INSTR(c4.expirationdate, '-', INSTR(c4.expirationdate, '-') + 1) + 1, 4);
				t_expdate_string := t_day_part||'-'||t_month_part||'-'||t_year_part;

				IF LENGTH(t_day_part) = 2 AND LENGTH(t_month_part) = 2 AND LENGTH(t_year_part) = 4 THEN --length check
					IF TO_NUMBER(t_day_part) BETWEEN 1 AND 31 AND TO_NUMBER(t_month_part) BETWEEN 1 AND 12 AND SUBSTR(t_year_part,1,2) IN ('18','19','20') THEN
						t_expirationdate := TO_DATE(t_expdate_string, 'DD-MM-YYYY');
					ELSE
						t_expirationdate := NULL;
					END IF;
				END IF;

				DXO.GETPKVALUE('ORION_SECMASTER_INV_INT',t_secmasterid);
				INSERT INTO dxo.ORION_SECMASTER_INV_INT(secmasterid, jobid, fundfamily, expirationdate, assetclass, isin, orionprodid, productname, symbol, cusip, producttype, assetclassdesc, price, pricedate, prodsubtype, parentprodid, prodvacode)
				VALUES (t_secmasterid, t_jobid, c4.fundfamily, t_expirationdate, C4.assetclass, c4.isin, c4.orionprodid, c4.productname, c4.ticker, c4.cusip, dxo.getorionproducttype(c4.producttype, c4.prodsubtype), c4.assetclassdesc, t_price, TO_DATE(c4.pricedate,'YYYY/MM/DD'), DXO.GetOrionProductsubtype(C4.prodsubtype), c4.parentprodid,c4.ticker );
				COMMIT;
				
				IF DXO.GetOrionProducttype(C4.PRODUCTTYPE, C4.PRODSUBTYPE) = '1006' AND C4.ParentProdID IS NOT NULL THEN
					T_PRODVACODE := C4.TICKER;
				ELSE
					T_PRODVACODE := NULL;
				END IF;
				-- Processing SO-Secmaster 
				BEGIN
					DXO.GETPKVALUE('ORION_SOSECMSATER_INV_INT',t_sosecmasterid);
					INSERT INTO dxo.ORION_SOSECMSATER_INV_INT (SOSECMASTERID, JOBID, SYMBOL, CUSIP, SECMASTERID, STATUS, SECTYPE, PRODVACODE, PPSTATUSMSG)
					VALUES (t_sosecmasterid, T_JOBID, C4.TICKER, C4.CUSIP, t_secmasterid, 0, DXO.GetOrionProducttype(C4.PRODUCTTYPE, C4.PRODSUBTYPE), T_PRODVACODE, 'Successful');
					COMMIT;
				EXCEPTION
					WHEN OTHERS THEN
					t_errmsg := SUBSTR(SQLERRM||', '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,200);
					t_errcode := SQLCODE;
					UPDATE DXO.ORION_SOSECMSATER_INV_INT SET PPSTATUS = 2, PPSTATUSMSG = t_errcode||' : '||t_errmsg WHERE JOBID = T_JOBID AND SOSECMASTERID = t_sosecmasterid;
					COMMIT;
				END;
			END IF;
		END;
		-- Processing for Pricehistory
		select count(*) into t_count2 FROM dxo.orion_pricehistory_inv_int WHERE jobid = t_jobid and price = c4.price and pricedate = TO_DATE(c4.pricedate,'YYYY/MM/DD');
		IF t_count2 = 0 then
			dxo.getpkvalue('ORION_PRICEHISTORY_INV_INT',t_pricehistoryid);
			INSERT INTO dxo.orion_pricehistory_inv_int(pricehistoryid, jobid, cusip, price, pricedate)
			VALUES (t_pricehistoryid, t_jobid, c4.cusip, t_price ,TO_DATE(c4.pricedate,'YYYY/MM/DD'));
			COMMIT;
		END IF;
		-- Update flag 1 in stg table (Processed)
		UPDATE dxo.orion_secmaster_inv_stg SET flag = 1 WHERE jobid = t_jobid and secmasterid = c4.secmasterid;
		COMMIT;
	END LOOP; --C4

	--- Skeleton record creation on the basis of matching CUSIP and SYMBOL
	FOR C5 IN ( SELECT p.positionid, p.investmentname, p.cusip, p.symbol FROM dxo.orion_position_inv_int p WHERE p.jobid = t_jobid AND NOT EXISTS( SELECT 1 FROM dxo.orion_secmaster_inv_int s WHERE s.jobid = t_jobid AND s.jobid = p.jobid AND UPPER(NVL(s.cusip,'0')) = UPPER(NVL(p.cusip,'0')) AND UPPER(NVL(s.symbol,'0')) = UPPER(NVL(p.symbol,'0'))) AND p.ppstatus = 1)
	LOOP
		dxo.getpkvalue('ORION_SECMASTER_INV_INT',t_secmasterid);
		INSERT INTO dxo.orion_secmaster_inv_int (secmasterid, jobid, productname, cusip, symbol)
		VALUES (t_secmasterid, t_jobid, c5.investmentname, c5.cusip, c5.symbol);
		COMMIT;
		dxo.getpkvalue('ORION_SOSECMSATER_INV_INT',t_sosecmasterid);
		INSERT INTO dxo.orion_sosecmsater_inv_int (sosecmasterid, jobid, symbol, cusip, secmasterid)
		VALUES (t_sosecmasterid, t_jobid, c5.symbol, c5.cusip, t_secmasterid);
		COMMIT;
		
		UPDATE dxo.orion_position_inv_int SET ppstatus = 3, ppstatusmsg = 'Skeleton record created.' WHERE jobid = t_jobid AND positionid = c5.positionid;
		COMMIT;
	END LOOP;
END;
/
