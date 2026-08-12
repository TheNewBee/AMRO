import { HttpClient } from '@angular/common/http';
import { inject, Injectable, InjectionToken } from '@angular/core';
import { Observable } from 'rxjs';
import { SummaryRow } from './summary.model';

export const API_BASE_URL = new InjectionToken<string>('API_BASE_URL', {
  providedIn: 'root',
  factory: () => '',
});

@Injectable({ providedIn: 'root' })
export class SummaryService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = inject(API_BASE_URL).replace(/\/$/, '');

  readonly csvUrl = `${this.baseUrl}/api/summary.csv`;

  getSummary(): Observable<SummaryRow[]> {
    return this.http.get<SummaryRow[]>(`${this.baseUrl}/api/summary`);
  }
}
