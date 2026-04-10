import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { ChatService } from './chat.service';
import { marked } from 'marked';

interface Message {
  text: string;
  sender: 'user' | 'agent';
  timestamp: Date;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss'
})
export class AppComponent {
  title = 'Multi-DB AI Chat';
  userMessage = '';
  messages: Message[] = [
    {
      text: 'Hello! I can help you query the Users and Orders databases. What would you like to know?',
      sender: 'agent',
      timestamp: new Date()
    }
  ];
  isLoading = false;
  currentStatus = '';

  constructor(private chatService: ChatService, private sanitizer: DomSanitizer) { }

  renderMarkdown(text: string): SafeHtml {
    const html = marked.parse(text) as string;
    return this.sanitizer.bypassSecurityTrustHtml(html);
  }

  async sendMessage() {
    if (!this.userMessage.trim() || this.isLoading) return;

    const messageContent = this.userMessage.trim();
    this.messages.push({
      text: messageContent,
      sender: 'user',
      timestamp: new Date()
    });

    this.userMessage = '';
    this.isLoading = true;
    this.currentStatus = '';

    // Create a placeholder for the agent's response
    const agentMessage: Message = {
      text: '',
      sender: 'agent',
      timestamp: new Date()
    };
    this.messages.push(agentMessage);

    try {
      const response = await this.chatService.sendMessageStream(messageContent);

      if (!response.body) {
        throw new Error('No response body');
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let leftoverBuffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value, { stream: true });
        const combinedChunk = leftoverBuffer + chunk;
        const lines = combinedChunk.split('\n');

        // The last element might be an incomplete JSON string
        leftoverBuffer = lines.pop() || '';

        for (const line of lines) {
          if (!line.trim()) continue;

          try {
            const data = JSON.parse(line);

            if (data.type === 'token') {
              agentMessage.text += data.content;
            } else if (data.type === 'tool_start') {
              this.currentStatus = `Searching ${data.tool}...`;
            } else if (data.type === 'tool_end') {
              this.currentStatus = '';
            } else if (data.type === 'error') {
              agentMessage.text = `Error: ${data.content}`;
            }

            this.scrollToBottom();
          } catch (e) {
            console.error('Error parsing JSON chunk from line:', line, e);
          }
        }
      }

    } catch (err) {
      console.error(err);
      agentMessage.text = 'Sorry, I encountered an error connecting to the server.';
    } finally {
      this.isLoading = false;
      this.currentStatus = '';
      this.scrollToBottom();
    }
  }

  scrollToBottom() {
    setTimeout(() => {
      const chatContainer = document.querySelector('.chat-messages');
      if (chatContainer) {
        chatContainer.scrollTop = chatContainer.scrollHeight;
      }
    }, 100);
  }
}

