package realtime;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class AlertMail {

	final String username = "alertt725@gmail.com";
	final String password = "password123#";

	Properties props = new Properties();
	Session session;
	private static AlertMail mailObject;
	public static AlertMail getMailObj(){
		if (mailObject==null){
			mailObject = new AlertMail();
		}
		return mailObject;
	}
	private AlertMail(){
//		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		session = Session.getInstance(props,
		  new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		  });
	}
	
	public void sendMail(String recepient, String subject, String content) {
		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress("from-email@gmail.com"));
			message.setRecipients(Message.RecipientType.TO,
					InternetAddress.parse(recepient));
				message.setSubject(subject);
				message.setText(content);


			Transport.send(message);

			System.out.println("Mailed user:"+recepient+" regarding:"+subject);

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void main(String args[]){
		System.out.println("Hello World");
		AlertMail mailObj = AlertMail.getMailObj();
		mailObj.sendMail("alertt725@gmail.com", "(Error)Tax Calculator Module Malfunctioned", "Hello User,\nThe Tax calculator has detected a exception \nSeverity Level: ERROR \nUser : John\nModule: State Tax Calculator\nCode Location:ComputeStateTax:18 \n\nThanks,\nDev Team");
	}
}