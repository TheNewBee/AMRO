import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting, HttpTestingController } from '@angular/common/http/testing';
import { App } from './app';
import { API_BASE_URL } from './summary.service';
import { SummaryRow } from './summary.model';

describe('App', () => {
  let fixture: ComponentFixture<App>;
  let http: HttpTestingController;

  const rows: SummaryRow[] = [
    {
      clientInformation: 'CL|1234|0002|0001',
      productInformation: 'SGX|FU|NK|20100910',
      totalTransactionAmount: '-52',
    },
  ];

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [App],
      providers: [provideHttpClient(), provideHttpClientTesting(), { provide: API_BASE_URL, useValue: '' }],
    }).compileComponents();
    fixture = TestBed.createComponent(App);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    fixture.destroy();
    http.verify({ ignoreCancelled: true });
  });

  it('shows loading, then renders accessible summary rows and refresh time', fakeAsync(() => {
    fixture.detectChanges();
    tick();
    expect(fixture.nativeElement.textContent).toContain('Loading summary');

    http.expectOne('/api/summary').flush(rows);
    fixture.detectChanges();

    const element = fixture.nativeElement as HTMLElement;
    expect(element.querySelector('table')).toBeTruthy();
    expect(element.querySelectorAll('th').length).toBe(3);
    expect(element.textContent).toContain('Client Information');
    expect(element.textContent).toContain('CL|1234|0002|0001');
    expect(element.textContent).toContain('Last refreshed');
    http.match('/api/summary').forEach((request) => request.flush(rows));
  }));

  it('does not overlap an in-flight request during polling', fakeAsync(() => {
    fixture.detectChanges();
    tick();
    const firstRequest = http.expectOne('/api/summary');

    tick(5000);
    http.expectNone('/api/summary');

    firstRequest.flush(rows);
    tick(5000);
    http.match('/api/summary').forEach((request) => request.flush(rows));
  }));

  it('shows an actionable error and can recover on the next poll', fakeAsync(() => {
    fixture.detectChanges();
    tick();
    http.expectOne('/api/summary').flush('offline', { status: 503, statusText: 'Unavailable' });
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain('Unable to load summary');
    expect(fixture.nativeElement.querySelector('table')).toBeTruthy();

    tick(5000);
    const request = http.expectOne('/api/summary');
    request.flush(rows);
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain('CL|1234|0002|0001');
  }));

  it('points the download action at the CSV endpoint', fakeAsync(() => {
    fixture.detectChanges();
    tick();
    const link = (fixture.nativeElement as HTMLElement).querySelector('a[download]') as HTMLAnchorElement;
    expect(link.href).toContain('/api/summary.csv');
    expect(link.download).toBe('Output.csv');
    http.match('/api/summary').forEach((request) => request.flush([]));
  }));
});
