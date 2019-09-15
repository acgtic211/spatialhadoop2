package edu.umn.cs.spatialHadoop.operations.dendis;

public class DENDIS {
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  public double[][] SamplingApproachP_DENDIS(double[][] data,int n,int p,double Granularity, Integer N_instance, int[] weight, int randomInit)
  {
    int[] Liste_instance;
    int[][] Liste;
    ParamFCNNrule P;
    int MaxCluster,N_prototype;
    Double dmax = Double.POSITIVE_INFINITY;

    MaxCluster = N_instance;
    Liste_instance = new int[MaxCluster];
    Liste = new int[MaxCluster][];

    P = new ParamFCNNrule(n,p);
    P.InitSingleParamFCNNrule();
    P.p = p;
    P.n = n;

    P.Option_Alea_Cleaning_2 = 0;
    P.Option_Alea_Cleaning_1 = 0;
    P.SansStep2 = 1;
    P.Init_Aleatoire = randomInit;
    P.Option_Assymptotique = 1;
    P.Auto = 1;
    P.L_pattern = 1;
    P.Save = 0;
    P.Dist_Connexity = 1;//li� � la moyenne
    P.Barycentre = 1;
    P.Granularity = Granularity;
    P.TradeoffDensite = 1;
    P.FactorDistance = 1;
    P.NbPointsMin = (int)Math.max(5,(double)(n)*P.Granularity); //Agit sur le nombre de prototypes/
    P.OccMin = (int)((double)(n) * P.Granularity);
    P.NbPointsNoise = (int)Math.max(1,(double)(n)/1000);//bruit
    P.DensiteMin = (int)((double)(n)*P.Granularity);//le nombre de patterns par occurence.
    P.MaxCluster = MaxCluster;

//	type = 1; // on raisonne sur les distances sur type = 0, sur les points si type 1.
//	OptionPreliminaire = 0;
    P.S2_Beta = 1;

    N_prototype = SamplingApproach_DENDIS(data,Liste_instance,weight,Liste,dmax,P);

    return SelectInstanceWithListe(data,p,Liste_instance,N_prototype,0,null);

  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  int SamplingApproach_DENDIS(double [][]data,int[] Liste_instance, int[] weight,
                              int[][] Liste, Double dmax, ParamFCNNrule P)
  {
    Internal I;
    int terminate;
    int New_instance;

    if (P==null)
      return 0;

    I = new Internal();
    //on initialise les data et on extrait le premier pattern soit al�atoire soit par min min.
    New_instance = I.InitProgramme(data,P);//New instance est le 1ere prototype, un des n data...

    terminate = 0;
    int N_instance = 0;
    while (terminate==0)
    {
      if (New_instance != -2)//vient de l'�tape inf�rieure o� on a stopp�...
      {

        N_instance++;//on augmente ici car on commence avec 1 instance.
        I.InitBoucle(N_instance-1,New_instance);//on initialise certains param�tres et surtout on disable les prototypes (cf. partie optimisation) dont les patterns ne peuvent pas �tre le futur prototype.
      }
      New_instance = I.FindNewInstanceMassique(data,P,N_instance);//d�termine le nouveau prototype et on r�ajuste les poids sur N_instance - 1. Le coeur du programme
      if (New_instance >= 0)//caution
      {
        I.CalculCost(P,N_instance);//optionnel ici.
        I.ProcessCriteria(N_instance,P);//on d�duit  le max(max) et min(max) � partir des infos de la ligne pr�c�dente. (on a la distribution des max sur s)
        I.UpdateAlgorithm(N_instance,New_instance);//l'instance s�lectionn�e ne peut plus �tre s�lectionn�..elle est marqu�e. On stocke le prototype s�lectionn�e dans le vecteur des prototypes.
        terminate = I.TestFin_DENDIS(P,N_instance);//test de la fin suivant le crit�re d'occurence et si mode Auto sur la distance de r�f�rence.
        if (terminate == 0)
          I.Update1knnPrototypeMasse(data,P,N_instance);//Update des 1knn entre prototype et la moyenne. En mode auto, on a tjs besoin cf in�galit� triangulaire
      }

      if (terminate == 1)// Termin� suivant les crit�res param�tr�s, on va d�graisser ou engraisser le nombre de prototypes.
      {
        I.AttributionPattern(N_instance,Liste,P);//on remplit les patterns attach�s � chaque instance. Aucun calcul de distance. Chaque pattern conna�t son prototype.
        Utils.SortPrototypes_DENDIS(I,P,N_instance,Liste,P.NbPointsNoise);//on enl�ve les protoypes avec le LABEL NOISE.
      }
    }

    if (P.Barycentre == 1)//dans ce cas on prend le barycentre..C'est une option inutilis�e. Au lieu de prendre le proto on prend le barycentre des patterns attach�s.
      Utils.UpdateWithGravity_DENDIS(data,I,P,N_instance);


    for (int i=0;i<N_instance;i++)//aspect informatique uniquement
    {
      Liste_instance[i] = I.Liste_selectionne[i];
      weight[i] = I.Weight[i];
    }

    return N_instance;
  }

  private double[][] SelectInstanceWithListe(double [][]pattern, int p, int[]liste_det, int n_inst, int Min, int[] weightG)
  {
    int i,j;
    int Nb=0;

    double[][] data = Utils.Reservmemdouble(n_inst,p);

    if (weightG != null)
    {
      for (i=0;i<n_inst;i++)
      {
        if (weightG[i] >= Min)
        {
          for (j=0;j<p;j++)
            data[Nb][j] =  pattern[liste_det[i]][j];
          weightG[Nb] = weightG[i];
          Nb++;
        }
      }
    }
    else
    {
      for (i=0;i<n_inst;i++)
      {
        for (j=0;j<p;j++)
          data[Nb][j] =  pattern[liste_det[i]][j];
        Nb++;
      }
    }

    return data;
  }


}
