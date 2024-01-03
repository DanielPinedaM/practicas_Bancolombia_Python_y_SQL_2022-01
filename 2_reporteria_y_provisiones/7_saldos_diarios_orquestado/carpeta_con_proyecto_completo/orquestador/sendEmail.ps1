# SendEmail.ps1 - Inicio del script

#Parameter declaration and definition
param(
   [Parameter(Mandatory=$True)]
   [string]$mails,
	
   [Parameter(Mandatory=$True)]
   [string]$subject,

   [Parameter(Mandatory=$True)]
   [string]$body,

   [Parameter(Mandatory=$False)]
   [array]$attachments,

   [Parameter(Mandatory=$True)]
   [array]$importance
)

# Busca si Outlook está iniciado, si no lo inicia.
$programName = "Outlook"
$isRunning = (Get-Process | Where-Object { $_.Name -eq $programName }).Count -gt 0
if (! $isRunning){Start-Process Outlook}

# Configuración del objeto 
$Outlook = New-Object -com Outlook.Application
$mail = $Outlook.CreateItem(0)
$mail.importance = $importance
$mail.subject = $subject
$mail.body = $body

# Agrego mails, separados por coma
$mail.To = $mails.Split(',')

# Agrego adjuntos, separados por barra
$attachments.Split("|") | ForEach-Object{
    $mail.Attachments.Add($_)
}

$mail.Send()

# give time to send the email
#Start-Sleep 20

# quit Outlook
#$Outlook.Quit()

exit


