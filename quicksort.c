#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#define QUEUE_SIZE 1000000
#define JOB 5000//  MEGEFOS TOY PINAKA
#define THREADS 4

#define WORK 0
#define FINISH 1
#define SUTDOWN 2


#define CUTOFF 10 //for inssort
#define EMPTY_ARG 32//random




void inssort(double *a,int n);
void quicksort(double *a,int n);
int partision(double *a,int n);




//H DOMH GIA THN OYRA
struct new_message{
    int type;
    int position;//start of array
    int possition_of_the_end;
};

// KAFOLIKH PINAKAS GIA THN OYRA
struct new_message new_queuen_message[QUEUE_SIZE];




// DIXTE GIA THN OYRA 
int qin = 0;
int qout = 0;
// POSA STOIXEIA YPARXOUN MESA STIN OYRA
int message_count = 0;
//MUTEX
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
// CONDITIONAL VARIABLE
pthread_cond_t msg_in = PTHREAD_COND_INITIALIZER;
pthread_cond_t msg_out = PTHREAD_COND_INITIALIZER;


void send(int type,int position,int possition_of_the_end)
{
    pthread_mutex_lock(&mutex);
    //AN GEMISH H OYRA KLHDOSE TON PRODUSER
    while(message_count >= QUEUE_SIZE)
    {
        printf("\nProducer locked\n");
        pthread_cond_wait(&msg_out,&mutex);
    }
    //printf("Producer sending %d: %d\n", type, position);
    //APOFHKESE TO MINHMA STHN OYRA TON ERGASION ME TA ANTISTIXA DEDOMENA 
    new_queuen_message[qin].type = type;
    new_queuen_message[qin].position = position;
    new_queuen_message[qin].possition_of_the_end = possition_of_the_end;
    qin++;
    //KYKLIKH OYRA
    if(qin>=QUEUE_SIZE)
    {
        qin = 0;
    }
    //AYXISE TO METRTI STON OYRA
    message_count++;
    //printf("new_queuen_message.type[%d]=%d\n",qin,new_queuen_message[qin].type);
    pthread_cond_signal(&msg_in);
    pthread_mutex_unlock(&mutex);
}


void recv(int *type,int *position,int *possition_of_the_end)
{
    pthread_mutex_lock(&mutex);
    //AN DEN EXEI H OYRA MINIMATA KLHDOSE DON CONSUMER
    while(message_count<1)
    {
        printf("I am hear in consumer is loced\n");
        pthread_cond_wait(&msg_in,&mutex);
    }
    //DIABASMA KAI ANANEOSH TON TIMON APO TO PAKETO THS OYRAS
    *type = new_queuen_message[qout].type;
    *position = new_queuen_message[qout].position;
    *possition_of_the_end = new_queuen_message[qout].possition_of_the_end;
    qout++;
    
    if(qout>=QUEUE_SIZE)
    {
        qout=0;
    }
    //MIOSE TON METRITH APO THN OYRA
    message_count--;
    printf("Consumer received %d: %d\n", *type, *position);
    pthread_cond_signal(&msg_out);
    pthread_mutex_unlock(&mutex);
    
}


void *thread_func(void *params)
{
    //ANAKTHSH PINAKA EISODOY APO TO KENTIKO THREAD
    double *arrayA;
    arrayA = (double*)params;
    
    int type,possition_of_the_end,partision_of_array;
    int position,size_of_array;
    
    //FERA TO TELEYTO MINIMA APO THN OYRA
    recv(&type,&position,&possition_of_the_end);
    while(1)
    {
        
        if(type == WORK)
        {
            //BRES TO UPOKOMATI POY PINAKA 
            size_of_array = possition_of_the_end - position;
            //AN TO YPOKOMATI TOY PINKA EINAI MIKROTERO KALESE TON insertion sort GIA NA KANEI THN TAXINOMHSH
            if(size_of_array<=CUTOFF)
            {
                inssort(arrayA+position,size_of_array);
                send(FINISH,position,possition_of_the_end);
                //STEILE MINIMA TERMATISMOY GIA TO KOMATI POY TAXINOMOIFIKE ME THN insertion sort GIA TO MAIN THREAD
            }
            else
            {
                //TO partision_of_array EINAI H METABLHTH STHN OPIA APOFHKEBETAI TO pivot APO TON PINAKA TON OPOIO EPEJERGAZOMASTE
                partision_of_array =  partision(arrayA+position,size_of_array);
                //ENA NEO MHNHMA GIA THN OYRA ME TO ARISTERO MEROS TOY PINAK STON OPIO XORISAME 
                send(WORK,position,position+partision_of_array);
                //KAI TO ANTOISTIXO GIA TO DEXI MEROS TOY PINAKA
                send(WORK,position+partision_of_array,possition_of_the_end);
            }
            
        }
        else if(type == SUTDOWN)
        {
            //STELE MINIMA GIA NA KLOISOYN TA THREADS
            send(SUTDOWN,0,EMPTY_ARG);
            break;
        }
        else
        {
            // AN TO type EINAI FINISH STOILE TO MINIMA
            send(type,position,possition_of_the_end);
        }
        //FERE TO TELEFTEO MINIMATA APO THN OYRA 
        recv(&type,&position,&possition_of_the_end);
    }
    

    pthread_exit(NULL);
}

int main(int argc, char** argv)
{
    //DIMIOYRGISE ENAN PINAKA PO THREEADS
    pthread_t mythreads[THREADS];
    struct new_message params;
    int i =0;
    
    
    //DIMIOYRGISE DINAMIKA TO ARRAY POY UA PAEI GIA TAXINOMISH
    double *arrayA;
    arrayA = (double*) malloc(JOB * sizeof(double));
    //ANAFESE STON PINAKA TOXEA STOIXEIA
    srand(time(NULL));
    for(i=0;i<JOB;i++)
    {
        arrayA[i] = (double)rand()/RAND_MAX;
        
    }
    
    
    for (i=0;i<THREADS;i++)
    {
        //ELENXE AN DIMIOYRGIFIKAN SOSTA TA THREAD
        if(pthread_create(&mythreads[i],NULL,thread_func,arrayA) != 0)
        {
        printf("Create Error 1\n");
        exit(1);
        }
    }
    

    //STEILE TO PROTO MHNIMA POY PERIXEXEI THN ARXI KAI TO TELOS TOY PINAKA
    send(WORK,0,JOB);
    //H METABLITH COUNT METRAEI POSA PRAGMATA EXOYN TELIOSEI
    int count = 0;
    
   
    while (1){
        //PERNOYME THS NES PARAMETROYS APO THN OYRA
        recv(&params.type,&params.position,&params.possition_of_the_end);
        //AN TO tupe = FINISH
        if (params.type == FINISH) {
            count += params.possition_of_the_end - params.position;
            //count += end - start;
            //AN OLLA TA STOIXOIA TOY PINKA EXOYN TAXINOMIFH
            if (count == JOB){
                //STOILE MINIMA GIA NA TERMATISOYN TA THREADS
                send(SUTDOWN, 0,EMPTY_ARG);
                break;
            }
        } else {
            //SE KAFE ALLH PERIPTOSH BALE ENA KENOYRIO MINIMA STHN OYRA
            send(params.type, params.position,params.possition_of_the_end);
        }
        
    }
    
    

    //KLISE OLLA TA THREAD
    for(i = 0 ; i< THREADS;i++)
    {
        pthread_join(mythreads[i],NULL);
    }
    
    //ELENXOS TOY PINAKA
    for(i = 0 ; i < JOB ; i++)
    {
        //printf("arrayA[%d]=%f\n",i,arrayA[i]);
        if(arrayA[i]<arrayA[i-1])
        {
            printf("Error");
            break;
        }
    }
    //APOSESMES H PINAKA, MUTETEX KAI CODISIONAL VARIABLE
    free(arrayA);
    
    pthread_mutex_destroy(&mutex);
    
    pthread_cond_destroy(&msg_in);
    pthread_cond_destroy(&msg_out);
    return 0;   
}

void inssort(double *a,int n)
{
    int i,j;
    double temp;
    for(i = 1 ; i<n;i++)
    {
        j =i;
        while((j>0) && (a[j-1])>a[j] )
        {
            temp = a[j-1];
            a[j-1] = a[j];
            a[j] = temp;
            j--;
            
        }
    }
}

void quicksort(double *a,int n)
{
    int i;
    i = partision(a,n);
    quicksort(a,i);
    quicksort(a+i,n-i);
}

int partision(double *a, int n)
{
    int i,j;
    int first =0;
    int middle = n/2;
    int last = n-1;
    double temp;
    
    if (a[first] > a[middle])
    {
        temp = a[first];
        a[first] = a[middle];
        a[middle] = temp;
    }
    if(a[middle] > a[last] )
    {
        temp = a[middle];
        a[middle] = a[last];
        a[last] = temp;
    }
    if (a[first] > a[middle])
    {
        temp = a[first];
        a[first] = a[middle];
        a[middle] = temp;
    }
    
    double p = a[middle];
    for(i=1,j=n-2;;i++,j--)
    {
        while(a[i]<p)i++;
        while(a[j]>p)j--;
        if (i>=j)break;
        temp = a[i];
        a[i] = a[j];
        a[j] = temp;
        
    } 
    return i;
    
}

