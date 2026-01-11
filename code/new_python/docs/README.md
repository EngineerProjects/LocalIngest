# 📚 Documentation - Construction Data Pipeline

This folder contains **business-focused documentation** that explains what the pipeline does, without technical code details.

---

## 🎯 Quick Navigation

### For Business Users
Start here if you want to understand **what** the pipeline does:

1. **[Business Calculations](calculs_assurance_construction.md)** ⭐ Most important
   - Explains all insurance calculations in plain language
   - Portfolio movements (new policies, terminations, renewals)
   - Capital amounts and coverage limits
   - Premium calculations

2. **[Workflows](workflows/)** - How data flows through each pipeline
   - Portfolio Movements workflow
   - Capital Extraction workflow  
   - Premium Emissions workflow

### For Data / Analytics Users  
Check these for understanding inputs and outputs:

3. **[Data Catalog](configs/Data_Catalog.md)** - All input/output tables
   - Monthly files
   - Reference data
   - Output datasets

4. **[Available Data](infos/available_datas.md)** - What's currently in the datalake

---

## 📖 Documents Available

| Document | Who Should Read | What You'll Learn |
|----------|----------------|-------------------|
| [**Business Calculations**](calculs_assurance_construction.md) | Everyone | Insurance formulas and business logic |
| [PTF_MVT Workflow](workflows/PTF_MVT_Workflow.md) | Analysts, Business | Portfolio movements process |
| [Capitaux Workflow](workflows/Capitaux_Workflow.md) | Analysts, Business | Capital extraction process |
| [Emissions Workflow](workflows/Emissions_Workflow.md) | Analysts, Business | Premium emissions process |
| [Data Catalog](configs/Data_Catalog.md) | Data teams | Input/output table reference |
| [Available Data](infos/available_datas.md) | Data teams | Current datalake inventory |

---

## 🔑 Key Insurance Concepts

| Term | Meaning | Example |
|------|---------|---------|
| **AFN** (Affaire Nouvelle) | New policy | Customer signs a new insurance contract |
| **RES** (Résiliation) | Termination | Customer cancels their contract |
| **PTF** (Portefeuille) | Active Portfolio | All policies currently in force |
| **SMP** | Maximum claim amount | Biggest claim we might have to pay |
| **LCI** | Contract limit | Maximum amount stated in contract |
| **PE** (Perte d'Exploitation) | Business interruption | Coverage for lost revenue if business stops |
| **RD** (Risque Direct) | Direct damage | Coverage for physical damage to property |
| **Coassurance** | Risk sharing | Multiple insurers share the same policy |

For complete definitions, see [Business Calculations](calculs_assurance_construction.md).

---

## 🚀 How to Use This Documentation

1. **New to the project?**
   - Start with [Business Calculations](calculs_assurance_construction.md)
   - Then read the workflow for your area of interest

2. **Need to find a specific table?**
   - Check [Data Catalog](configs/Data_Catalog.md)
   - Or [Available Data](infos/available_datas.md)

3. **Want to understand a calculation?**
   - Go to [Business Calculations](calculs_assurance_construction.md)
   - Search for the metric (e.g., "AFN", "SMP", "Exposure")

---

## 💡 Important Notes

- All documentation is **business-focused** - no programming code
- Formulas are explained in **plain language** with examples
- Technical implementation details are in the code comments, not here
