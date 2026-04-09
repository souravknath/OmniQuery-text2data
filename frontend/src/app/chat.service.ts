import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class ChatService {
  private apiUrl = 'http://localhost:8000/chat';

  constructor() { }

  /**
   * Sends a message to the backend and returns a Response object
   * that can be used to read a Stream.
   */
  async sendMessageStream(message: string): Promise<Response> {
    return fetch(this.apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ message }),
    });
  }
}

