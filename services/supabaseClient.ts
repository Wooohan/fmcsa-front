import {
  saveCarrierToBackend,
  saveCarriersToBackend,
  fetchCarriersFromBackend,
  deleteCarrierFromBackend,
  getCarrierCountFromBackend,
  updateCarrierInsuranceInBackend,
  updateCarrierSafetyInBackend,
  CarrierFilters,
} from './backendApiService';

// ── Census-schema carrier record shape (matches database.py _carrier_row_to_dict) ──
export interface CarrierRecord {
  // IDs
  mc_number?: string | null;
  dot_number: string;
  duns_number?: string | null;
  // Identity
  legal_name: string;
  dba_name?: string | null;
  entity_type: string;
  status: string;
  // Contact
  email?: string | null;
  phone?: string | null;
  company_rep?: string | null;
  // Location
  physical_address?: string | null;
  mailing_address?: string | null;
  phy_state?: string | null;
  // Compliance
  mcs150_date?: string | null;
  mcs150_mileage?: string | null;
  add_date?: string | null;
  date_scraped?: string | null;
  // Operations
  operation_classification?: string[];
  carrier_operation?: string[];
  cargo_carried?: string[];
  hm_ind?: string | null;
  // Fleet
  power_units?: string | null;
  drivers?: string | null;
  // Safety (enriched separately)
  safety_rating?: string | null;
  safety_rating_date?: string | null;
  basic_scores?: any;
  oos_rates?: any;
  out_of_service_date?: string | null;
  state_carrier_id?: string | null;
  inspections?: any;
  crashes?: any;
  // Insurance
  insurance_history_filings?: any[];
}

// ── Save helpers (kept for scraper compatibility) ─────────────────────────────

export const saveCarrierToSupabase = async (
  carrier: any
): Promise<{ success: boolean; error?: string; data?: any }> => {
  try {
    if (!carrier.dotNumber || !carrier.legalName) {
      return {
        success: false,
        error: 'Missing required fields: dotNumber or legalName',
      };
    }
    const record: any = {
      dot_number: carrier.dotNumber,
      legal_name: carrier.legalName,
      dba_name: carrier.dbaName || null,
      entity_type: carrier.entityType,
      status: carrier.status,
      email: carrier.email || null,
      phone: carrier.phone || null,
      power_units: carrier.powerUnits || null,
      drivers: carrier.drivers || null,
      physical_address: carrier.physicalAddress || null,
      mailing_address: carrier.mailingAddress || null,
      date_scraped: carrier.dateScraped,
      mcs150_date: carrier.mcs150Date || null,
      mcs150_mileage: carrier.mcs150Mileage || null,
      operation_classification: carrier.operationClassification || [],
      carrier_operation: carrier.carrierOperation || [],
      cargo_carried: carrier.cargoCarried || [],
      duns_number: carrier.dunsNumber || null,
      safety_rating: carrier.safetyRating || null,
      safety_rating_date: carrier.safetyRatingDate || null,
      basic_scores: carrier.basicScores || null,
      oos_rates: carrier.oosRates || null,
      inspections: carrier.inspections || null,
      crashes: carrier.crashes || null,
    };
    return saveCarrierToBackend(record);
  } catch (err: any) {
    console.error('Exception saving to Backend:', err);
    return { success: false, error: `Exception: ${err.message}` };
  }
};

export const saveCarriersToSupabase = async (
  carriers: any[]
): Promise<{ success: boolean; error?: string; saved: number; failed: number }> => {
  try {
    const records = carriers.map(carrier => ({
      dot_number: carrier.dotNumber,
      legal_name: carrier.legalName,
      dba_name: carrier.dbaName || null,
      entity_type: carrier.entityType,
      status: carrier.status,
      email: carrier.email || null,
      phone: carrier.phone || null,
      power_units: carrier.powerUnits || null,
      drivers: carrier.drivers || null,
      physical_address: carrier.physicalAddress || null,
      mailing_address: carrier.mailingAddress || null,
      date_scraped: carrier.dateScraped,
      mcs150_date: carrier.mcs150Date || null,
      mcs150_mileage: carrier.mcs150Mileage || null,
      operation_classification: carrier.operationClassification || [],
      carrier_operation: carrier.carrierOperation || [],
      cargo_carried: carrier.cargoCarried || [],
      duns_number: carrier.dunsNumber || null,
      safety_rating: carrier.safetyRating || null,
      safety_rating_date: carrier.safetyRatingDate || null,
    }));
    return saveCarriersToBackend(records);
  } catch (err: any) {
    console.error('Exception saving batch to Backend:', err);
    return { success: false, saved: 0, failed: carriers.length, error: `Exception: ${err.message}` };
  }
};

// ── Filters interface (mirrors backendApiService CarrierFilters) ───────────────

export interface CarrierFiltersSupabase {
  mcNumber?: string;
  dotNumber?: string;
  legalName?: string;
  entityType?: string;
  active?: string;
  state?: string;
  hasEmail?: string;
  hasBoc3?: string;
  hasCompanyRep?: string;
  yearsInBusinessMin?: number;
  yearsInBusinessMax?: number;
  classification?: string[];
  carrierOperation?: string[];
  hazmat?: string;
  powerUnitsMin?: number;
  powerUnitsMax?: number;
  driversMin?: number;
  driversMax?: number;
  cargo?: string[];
  insuranceRequired?: string[];
  bipdMin?: number;
  bipdMax?: number;
  insEffectiveDateFrom?: string;
  insEffectiveDateTo?: string;
  bipdOnFile?: string;
  cargoOnFile?: string;
  bondOnFile?: string;
  trustFundOnFile?: string;
  insCancellationDateFrom?: string;
  insCancellationDateTo?: string;
  oosMin?: number;
  oosMax?: number;
  crashesMin?: number;
  crashesMax?: number;
  injuriesMin?: number;
  injuriesMax?: number;
  fatalitiesMin?: number;
  fatalitiesMax?: number;
  towawayMin?: number;
  towawayMax?: number;
  inspectionsMin?: number;
  inspectionsMax?: number;
  insuranceCompany?: string[];
  renewalPolicyMonths?: string;
  renewalDateFrom?: string;
  renewalDateTo?: string;
  limit?: number;
  offset?: number;
}

// ── Main fetch — maps Census API response → CarrierData shape ─────────────────

export const fetchCarriersFromSupabase = async (
  filters: CarrierFiltersSupabase = {}
): Promise<{ data: any[]; filtered_count: number }> => {
  try {
    const backendFilters: CarrierFilters = {
      mcNumber: filters.mcNumber,
      dotNumber: filters.dotNumber,
      legalName: filters.legalName,
      entityType: filters.entityType,
      active: filters.active,
      state: filters.state,
      hasEmail: filters.hasEmail,
      hasBoc3: filters.hasBoc3,
      hasCompanyRep: filters.hasCompanyRep,
      classification: filters.classification,
      carrierOperation: filters.carrierOperation,
      cargo: filters.cargo,
      hazmat: filters.hazmat,
      powerUnitsMin: filters.powerUnitsMin,
      powerUnitsMax: filters.powerUnitsMax,
      driversMin: filters.driversMin,
      driversMax: filters.driversMax,
      insuranceRequired: filters.insuranceRequired,
      bipdMin: filters.bipdMin,
      bipdMax: filters.bipdMax,
      insEffectiveDateFrom: filters.insEffectiveDateFrom,
      insEffectiveDateTo: filters.insEffectiveDateTo,
      bipdOnFile: filters.bipdOnFile,
      cargoOnFile: filters.cargoOnFile,
      bondOnFile: filters.bondOnFile,
      trustFundOnFile: filters.trustFundOnFile,
      insCancellationDateFrom: filters.insCancellationDateFrom,
      insCancellationDateTo: filters.insCancellationDateTo,
      yearsInBusinessMin: filters.yearsInBusinessMin,
      yearsInBusinessMax: filters.yearsInBusinessMax,
      // Safety filters not supported by Census data — omit
      insuranceCompany: filters.insuranceCompany,
      renewalPolicyMonths: filters.renewalPolicyMonths,
      renewalDateFrom: filters.renewalDateFrom,
      renewalDateTo: filters.renewalDateTo,
      limit: filters.limit,
      offset: filters.offset,
    };

    const result = await fetchCarriersFromBackend(backendFilters);

    // Map Census API fields → CarrierData interface used by CarrierSearch.tsx
    const mapped = (result.data || []).map((record: any) => ({
      // ── Identification
      mcNumber: record.mc_number || '',
      dotNumber: record.dot_number || '',
      dunsNumber: record.duns_number || '',

      // ── Identity
      legalName: record.legal_name || '',
      dbaName: record.dba_name || '',
      entityType: record.entity_type || 'CARRIER',
      status: record.status || 'NOT AUTHORIZED',

      // ── Contact
      email: record.email || '',
      phone: record.phone || '',
      companyRep: record.company_rep || '',

      // ── Location
      physicalAddress: record.physical_address || '',
      mailingAddress: record.mailing_address || '',

      // ── Compliance
      mcs150Date: record.mcs150_date || '',
      mcs150Mileage: record.mcs150_mileage || '',
      dateScraped: record.date_scraped || '',

      // ── Operations
      operationClassification: record.operation_classification || [],
      carrierOperation: record.carrier_operation || [],
      cargoCarried: record.cargo_carried || [],

      // ── Fleet
      powerUnits: record.power_units || '',
      drivers: record.drivers || '',

      // ── Safety (not in Census — will be null/empty)
      safetyRating: record.safety_rating || '',
      safetyRatingDate: record.safety_rating_date || '',
      basicScores: record.basic_scores || [],
      oosRates: record.oos_rates || [],
      outOfServiceDate: record.out_of_service_date || '',
      stateCarrierId: record.state_carrier_id || '',
      inspections: record.inspections || [],
      crashes: record.crashes || [],

      // ── Insurance (from carriers.insurance JSONB)
      insuranceHistoryFilings: record.insurance_history_filings || [],
      insurancePolicies: record.insurance_history_filings || [],
    }));

    return { data: mapped, filtered_count: result.filtered_count };
  } catch (err: any) {
    console.error('Backend fetch error:', err);
    return { data: [], filtered_count: 0 };
  }
};

// ── Passthrough helpers ───────────────────────────────────────────────────────

export const deleteCarrierFromSupabase = async (mcNumber: string): Promise<boolean> => {
  return deleteCarrierFromBackend(mcNumber);
};

export const getCarrierCountFromSupabase = async (): Promise<number> => {
  return getCarrierCountFromBackend();
};

export const updateCarrierInsuranceInSupabase = async (
  dotNumber: string,
  policies: any[]
): Promise<boolean> => {
  return updateCarrierInsuranceInBackend(dotNumber, policies);
};

export const updateCarrierSafetyInSupabase = async (
  dotNumber: string,
  safetyData: any
): Promise<boolean> => {
  return updateCarrierSafetyInBackend(dotNumber, safetyData);
};

export const updateCarrierInsurance = updateCarrierInsuranceInSupabase;
