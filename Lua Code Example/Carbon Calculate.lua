function CarbonCal.main()
  -- Get Realtime Energy
  MDB001 = addr_getfloat("@MDB-001_Active Energy Delivered",1,2)
  CWPA = addr_getfloat("@CWP-A_Active Energy Delivered",1,2)
  CWPB = addr_getfloat("@CWP-B_Active Energy Delivered",1,2)
  CWPC = addr_getfloat("@CWP-C_Active Energy Delivered",1,2)
  TPA = addr_getfloat("@TP-A_Active Energy Delivered",1,2)
  TPB = addr_getfloat("@TP-B_Active Energy Delivered",1,2)
  TPC = addr_getfloat("@TP-C_Active Energy Delivered",1,2)
  -- Consumption Summary
  SupplyABC = CWPA + CWPB + CWPC
  TransferABC = TPA + TPB + TPC
  Other = MDB001 - (SupplyABC+TransferABC)
  addr_setfloat("@SupplyPumpABC",SupplyABC,1,2)
  addr_setfloat("@TransferPumpABC",TransferABC,1,2)
  addr_setfloat("@Other",Other,1,2)
  -- % usage calculate
  addr_setshort("@%SupplyPumpABC", string.format("%.0f", (SupplyABC * 10000)/MDB001))
  addr_setshort("@%TransferPumpABC", string.format("%.0f", (TransferABC * 10000)/MDB001))
  addr_setshort("@%Other", string.format("%.0f", (Other * 10000)/MDB001))
  
  -- Get Stamp Energy 
  MDB001_Stamp = addr_getfloat("@MDB-001-Stamp",1,2)
  CWPA_Stamp = addr_getfloat("@CWP-A-Stamp",1,2)
  CWPB_Stamp = addr_getfloat("@CWP-B-Stamp",1,2)
  CWPC_Stamp = addr_getfloat("@CWP-C-Stamp",1,2)
  TPA_Stamp = addr_getfloat("@TP-A-Stamp",1,2)
  TPB_Stamp = addr_getfloat("@TP-B-Stamp",1,2)
  TPC_Stamp = addr_getfloat("@TP-C-Stamp",1,2)
  -- Calculate Energy Daily
  MDB001_Daily = MDB001 - MDB001_Stamp
  CWPA_Daily = CWPA - CWPA_Stamp
  CWPB_Daily = CWPB - CWPB_Stamp
  CWPC_Daily = CWPC - CWPC_Stamp
  TPA_Daily = TPA - TPA_Stamp
  TPB_Daily = TPB - TPB_Stamp
  TPC_Daily = TPC - TPC_Stamp
  -- Record Energy Daily
  addr_setfloat("@MDB-001-Daily",MDB001_Daily,1,2)
  addr_setfloat("@CWP-A-Daily",CWPA_Daily,1,2)
  addr_setfloat("@CWP-B-Daily",CWPB_Daily,1,2)
  addr_setfloat("@CWP-C-Daily",CWPC_Daily,1,2)
  addr_setfloat("@TP-A-Daily",TPA_Daily,1,2)
  addr_setfloat("@TP-B-Daily",TPB_Daily,1,2)
  addr_setfloat("@TP-C-Daily",TPC_Daily,1,2)
  
  -- Calculate Carbon
  local CarbonValue = 0.4999
  MDB001_Carbon = MDB001_Daily*CarbonValue
  CWPA_Carbon = CWPA_Daily*CarbonValue
  CWPB_Carbon = CWPB_Daily*CarbonValue
  CWPC_Carbon = CWPC_Daily*CarbonValue
  TPA_Carbon = TPA_Daily*CarbonValue
  TPB_Carbon = TPB_Daily*CarbonValue
  TPC_Carbon = TPC_Daily*CarbonValue
  -- Record Carbon Daily
  addr_setfloat("@MDB-001-Carbon",MDB001_Carbon,1,2)
  addr_setfloat("@CWP-A-Carbon",CWPA_Carbon,1,2)
  addr_setfloat("@CWP-B-Carbon",CWPB_Carbon,1,2)
  addr_setfloat("@CWP-C-Carbon",CWPC_Carbon,1,2)
  addr_setfloat("@TP-A-Carbon",TPA_Carbon,1,2)
  addr_setfloat("@TP-B-Carbon",TPB_Carbon,1,2)
  addr_setfloat("@TP-C-Carbon",TPC_Carbon,1,2)
  
  -- Daily Consumption Summary update 25/01/2024
  DailySupplyABC = CWPA_Daily + CWPB_Daily + CWPC_Daily
  DailyTransferABC = TPA_Daily + TPB_Daily + TPC_Daily
  DailyOther = MDB001_Daily - (DailySupplyABC+DailyTransferABC)
  addr_setfloat("@DailySupplyABC",DailySupplyABC,1,2)
  addr_setfloat("@DailyTransferABC",DailyTransferABC,1,2)
  addr_setfloat("@DailyOther",DailyOther,1,2)
  
  -- % Daily usage calculate
  addr_setshort("@%DailySupplyPumpABC", string.format("%.0f", (DailySupplyABC * 10000)/MDB001_Daily))
  addr_setshort("@%DailyTransferPumpABC", string.format("%.0f", (DailyTransferABC * 10000)/MDB001_Daily))
  addr_setshort("@%DailyOther", string.format("%.0f", (DailyOther * 10000)/MDB001_Daily))
end