import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting, HttpTestingController } from '@angular/common/http/testing';
import { API_BASE_URL, SummaryService } from './summary.service';
import { SummaryRow } from './summary.model';

describe('SummaryService', () => {
  let service: SummaryService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: API_BASE_URL, useValue: '' },
        SummaryService,
      ],
    });
    service = TestBed.inject(SummaryService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('gets summary rows from the relative API endpoint', () => {
    const rows: SummaryRow[] = [
      {
        clientInformation: 'CL|1234|0002|0001',
        productInformation: 'SGX|FU|NK|20100910',
        totalTransactionAmount: '-52',
      },
    ];

    service.getSummary().subscribe((result) => expect(result).toEqual(rows));

    const request = http.expectOne('/api/summary');
    expect(request.request.method).toBe('GET');
    request.flush(rows);
  });

  it('uses a configured backend base URL for both API endpoints', () => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: API_BASE_URL, useValue: 'https://backend.example.test/' },
        SummaryService,
      ],
    });

    const configuredService = TestBed.inject(SummaryService);
    expect(configuredService.csvUrl).toBe('https://backend.example.test/api/summary.csv');

    configuredService.getSummary().subscribe();

    const request = TestBed.inject(HttpTestingController).expectOne(
      'https://backend.example.test/api/summary',
    );
    request.flush([]);
  });
});
