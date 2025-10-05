function EnergyStamp.main()
  --dosomething
  local HMS=os.date("%H%M%S", os.time() - 3600)
  --print(HMS.." ####")
  
  local MDB=addr_getfloat("@MDB-1_Active Energy Delivered",1,2)
  local DB1=addr_getfloat("@DB-1_Active Energy Delivered",1,2)
  local DB2=addr_getfloat("@DB-2_Active Energy Delivered",1,2)
  local DB3=addr_getfloat("@DB-3_Active Energy Delivered",1,2)
  local DB4=addr_getfloat("@DB-4_Active Energy Delivered",1,2)
  local DB5=addr_getfloat("@DB-5_Active Energy Delivered",1,2)
  
  local MDBStamp=addr_getfloat("@MDB_Stamp_0900",1,2)
  local DB1Stamp=addr_getfloat("@DB1_Stamp_0900",1,2)
  local DB2Stamp=addr_getfloat("@DB2_Stamp_0900",1,2)
  local DB3Stamp=addr_getfloat("@DB3_Stamp_0900",1,2)
  local DB4Stamp=addr_getfloat("@DB4_Stamp_0900",1,2)
  local DB5Stamp=addr_getfloat("@DB5_Stamp_0900",1,2)
  
  local MDBOP=addr_getfloat("@MDB_On_Peak",1,2)
  local DB1OP=addr_getfloat("@DB1_On_Peak",1,2)
  local DB2OP=addr_getfloat("@DB2_On_Peak",1,2)
  local DB3OP=addr_getfloat("@DB3_On_Peak",1,2)
  local DB4OP=addr_getfloat("@DB4_On_Peak",1,2)
  local DB5OP=addr_getfloat("@DB5_On_Peak",1,2)
  
  local MDBFP=addr_getfloat("@MDB_Off_Peak",1,2)
  local DB1FP=addr_getfloat("@DB1_Off_Peak",1,2)
  local DB2FP=addr_getfloat("@DB2_Off_Peak",1,2)
  local DB3FP=addr_getfloat("@DB3_Off_Peak",1,2)
  local DB4FP=addr_getfloat("@DB4_Off_Peak",1,2)
  local DB5FP=addr_getfloat("@DB5_Off_Peak",1,2)
  
  if(MDB~=nil) and (DB1~=nil) and (DB2~=nil) and (DB3~=nil) and (DB4~=nil) and (DB5~=nil) then
      -- Stamp
      if(HMS=='090000') then 
          addr_setfloat("@MDB_Stamp_0900",MDB,1,2)
          addr_setfloat("@DB1_Stamp_0900",DB1,1,2)
          addr_setfloat("@DB2_Stamp_0900",DB2,1,2)
          addr_setfloat("@DB3_Stamp_0900",DB3,1,2)
          addr_setfloat("@DB4_Stamp_0900",DB4,1,2)
          addr_setfloat("@DB5_Stamp_0900",DB5,1,2)
      end
      -- Trigger Usage Daily Report
      if(HMS=='090500') then 
          addr_setbit("@Daily_Trigger",1)
      end
      -- Reset Trigger Usage Daily Report
      if(HMS=='092000') then 
          addr_setbit("@Daily_Trigger",0)
      end
      -- On Peak
      if(HMS=='220000') then
          addr_setfloat("@MDB_On_Peak",MDB-MDBStamp,1,2)
          addr_setfloat("@DB1_On_Peak",DB1-DB1Stamp,1,2)
          addr_setfloat("@DB2_On_Peak",DB2-DB2Stamp,1,2)
          addr_setfloat("@DB3_On_Peak",DB3-DB3Stamp,1,2)
          addr_setfloat("@DB4_On_Peak",DB4-DB4Stamp,1,2)
          addr_setfloat("@DB5_On_Peak",DB5-DB5Stamp,1,2)
          
          addr_setfloat("@MDB_Off_Peak",0.000,1,2)
          addr_setfloat("@DB1_Off_Peak",0.000,1,2)
          addr_setfloat("@DB2_Off_Peak",0.000,1,2)
          addr_setfloat("@DB3_Off_Peak",0.000,1,2)
          addr_setfloat("@DB4_Off_Peak",0.000,1,2)
          addr_setfloat("@DB5_Off_Peak",0.000,1,2)
      end
      -- Off Peak
      if(HMS=='085955') then
          addr_setfloat("@MDB_Off_Peak",(MDB-MDBStamp)-MDBOP,1,2)
          addr_setfloat("@DB1_Off_Peak",(DB1-DB1Stamp)-DB1OP,1,2)
          addr_setfloat("@DB2_Off_Peak",(DB2-DB2Stamp)-DB2OP,1,2)
          addr_setfloat("@DB3_Off_Peak",(DB3-DB3Stamp)-DB3OP,1,2)
          addr_setfloat("@DB4_Off_Peak",(DB4-DB4Stamp)-DB4OP,1,2)
          addr_setfloat("@DB5_Off_Peak",(DB5-DB5Stamp)-DB5OP,1,2)
      end
      -- Sum Total Consumption
      if(HMS=='085958') then
          addr_setfloat("@MDB_Total_Cons_Yesterday",MDBOP+MDBFP,1,2)
          addr_setfloat("@DB1_Total_Cons_Yesterday",DB1OP+DB1FP,1,2)
          addr_setfloat("@DB2_Total_Cons_Yesterday",DB2OP+DB2FP,1,2)
          addr_setfloat("@DB3_Total_Cons_Yesterday",DB3OP+DB3FP,1,2)
          addr_setfloat("@DB4_Total_Cons_Yesterday",DB4OP+DB4FP,1,2)
          addr_setfloat("@DB5_Total_Cons_Yesterday",DB5OP+DB5FP,1,2)
      end
  end
end