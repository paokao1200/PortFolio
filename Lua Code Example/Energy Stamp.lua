function EnergyStamp.main()
    --dosomething
    addr_setbit("@DailyTrigger",0)
    addr_setbit("@UsageTrigger",0)
    local HMS=os.date("%H%M%S", os.time() - 3600)
    print(HMS)
    if(HMS=='060000') then --StampEnergyReport
        addr_setbit("@DailyTrigger",1)
        addr_setbit("@UsageTrigger",1)
    end
    if(HMS=='060005') then
        MDB001 = addr_getfloat("@MDB-001_Active Energy Delivered",1,2)
        CWPA = addr_getfloat("@CWP-A_Active Energy Delivered",1,2)
        CWPB = addr_getfloat("@CWP-B_Active Energy Delivered",1,2)
        CWPC = addr_getfloat("@CWP-C_Active Energy Delivered",1,2)
        TPA = addr_getfloat("@TP-A_Active Energy Delivered",1,2)
        TPB = addr_getfloat("@TP-B_Active Energy Delivered",1,2)
        TPC = addr_getfloat("@TP-C_Active Energy Delivered",1,2)
        addr_setfloat("@MDB-001-Stamp",MDB001,1,2)
        addr_setfloat("@CWP-A-Stamp",CWPA,1,2)
        addr_setfloat("@CWP-B-Stamp",CWPB,1,2)
        addr_setfloat("@CWP-C-Stamp",CWPC,1,2)
        addr_setfloat("@TP-A-Stamp",TPA,1,2)
        addr_setfloat("@TP-B-Stamp",TPB,1,2)
        addr_setfloat("@TP-C-Stamp",TPC,1,2)
    end
    --[[
    if(HMS=='235955') then --StampDailyUsageReport
        addr_setbit("@DailyTrigger",1)
    end
    --]]
end